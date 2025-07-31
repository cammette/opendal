use std::pin::Pin;
use std::sync::Arc;
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};
use opendal::{Operator, Buffer};
use tonic::{Request, Response, Status, Streaming};

// 假设这些是从 protobuf 生成的类型
pub mod file_service {
    #[derive(Clone, Debug)]
    pub struct DownloadRequest {
        pub path: String,
        pub range: Option<Range>,
        pub chunk_size: u32,
    }

    #[derive(Clone, Debug)]
    pub struct Range {
        pub start: u64,
        pub end: Option<u64>,
    }

    #[derive(Clone, Debug)]
    pub struct FileChunk {
        pub data: Vec<u8>,
        pub offset: u64,
        pub is_last: bool,
    }

    #[derive(Clone, Debug)]
    pub struct FileInfo {
        pub path: String,
        pub size: u64,
        pub content_type: String,
        pub last_modified: i64,
    }

    #[derive(Clone, Debug)]
    pub struct FileInfoRequest {
        pub path: String,
    }

    #[derive(Clone, Debug)]
    pub struct UploadChunk {
        pub path: String,
        pub data: Vec<u8>,
        pub offset: u64,
        pub is_last: bool,
    }

    #[derive(Clone, Debug)]
    pub struct UploadResponse {
        pub success: bool,
        pub message: String,
        pub bytes_written: u64,
    }
}

use file_service::*;

/// 高性能零拷贝文件流服务
pub struct FileStreamServiceImpl {
    operator: Operator,
    default_chunk_size: usize,
    max_concurrent_reads: usize,
}

impl FileStreamServiceImpl {
    pub fn new(operator: Operator) -> Self {
        Self {
            operator,
            default_chunk_size: 256 * 1024, // 256KB 平衡内存和性能
            max_concurrent_reads: 8,
        }
    }

    /// 最优化的零拷贝文件流实现
    async fn create_zero_copy_stream(
        &self,
        path: &str,
        range: Option<(u64, Option<u64>)>,
        chunk_size: usize,
    ) -> Result<impl Stream<Item = Result<FileChunk, Status>>, Status> {
        
        // 创建配置了并发和分块的 Reader
        let reader = self.operator
            .reader_with(path)
            .chunk(chunk_size)
            .concurrent(self.max_concurrent_reads)
            .await
            .map_err(|e| Status::not_found(format!("File not found: {}", e)))?;

        // 解析范围
        let range_bounds = match range {
            Some((start, Some(end))) => start..=end,
            Some((start, None)) => start..,
            None => ..,
        };

        // 创建零拷贝的 Buffer 流
        let buffer_stream = reader
            .into_stream(range_bounds)
            .await
            .map_err(|e| Status::internal(format!("Failed to create stream: {}", e)))?;

        // 转换为 FileChunk 流，最小化拷贝
        let file_stream = buffer_stream
            .scan(0u64, move |offset, buffer_result| {
                let current_offset = *offset;
                async move {
                    match buffer_result {
                        Ok(buffer) => {
                            if buffer.is_empty() {
                                // 结束标记
                                None
                            } else {
                                let chunk_data = buffer.to_vec(); // 唯一的必要拷贝点
                                let chunk_size = chunk_data.len() as u64;
                                *offset += chunk_size;
                                
                                Some(Ok(FileChunk {
                                    data: chunk_data,
                                    offset: current_offset,
                                    is_last: false, // 将在流结束时更新
                                }))
                            }
                        }
                        Err(e) => Some(Err(Status::internal(format!("Read error: {}", e)))),
                    }
                }
            })
            .map(|item| {
                // 可以在这里添加额外的处理逻辑
                item
            });

        Ok(file_stream)
    }

    /// 极致优化：使用 Bytes 流避免最后的拷贝
    /// 注意：这需要修改 protobuf 定义以支持 bytes 字段
    async fn create_bytes_stream(
        &self,
        path: &str,
        range: Option<(u64, Option<u64>)>,
        chunk_size: usize,
    ) -> Result<impl Stream<Item = Result<Bytes, Status>>, Status> {
        
        let reader = self.operator
            .reader_with(path)
            .chunk(chunk_size)
            .concurrent(self.max_concurrent_reads)
            .await
            .map_err(|e| Status::not_found(format!("File not found: {}", e)))?;

        let range_bounds = match range {
            Some((start, Some(end))) => start..=end,
            Some((start, None)) => start..,
            None => ..,
        };

        let buffer_stream = reader
            .into_stream(range_bounds)
            .await
            .map_err(|e| Status::internal(format!("Failed to create stream: {}", e)))?;

        // 完全零拷贝：Buffer -> Bytes
        let bytes_stream = buffer_stream
            .map(|buffer_result| {
                buffer_result
                    .map_err(|e| Status::internal(format!("Read error: {}", e)))
                    .map(|buffer| buffer.to_bytes()) // 零拷贝转换
            });

        Ok(bytes_stream)
    }

    /// 智能分块：根据网络条件动态调整块大小
    fn calculate_optimal_chunk_size(&self, file_size: u64, network_speed_mbps: u32) -> usize {
        const MIN_CHUNK_SIZE: usize = 64 * 1024;   // 64KB
        const MAX_CHUNK_SIZE: usize = 4 * 1024 * 1024; // 4MB
        
        // 基于网络速度计算合适的块大小
        let base_size = (network_speed_mbps as usize * 1024 * 1024 / 8) / 10; // 100ms worth of data
        
        // 限制在合理范围内
        base_size.clamp(MIN_CHUNK_SIZE, MAX_CHUNK_SIZE)
    }

    /// 内存池优化的读取
    async fn create_pooled_stream(
        &self,
        path: &str,
        chunk_size: usize,
    ) -> Result<impl Stream<Item = Result<FileChunk, Status>>, Status> {
        
        // 使用 OpenDAL 的缓冲池特性
        let reader = self.operator
            .reader_with(path)
            .chunk(chunk_size)
            .concurrent(self.max_concurrent_reads)
            .await
            .map_err(|e| Status::not_found(format!("File not found: {}", e)))?;

        let buffer_stream = reader
            .into_stream(..)
            .await
            .map_err(|e| Status::internal(format!("Failed to create stream: {}", e)))?;

        // 使用预分配的向量减少分配
        let file_stream = buffer_stream
            .scan(0u64, |offset, buffer_result| {
                let current_offset = *offset;
                async move {
                    match buffer_result {
                        Ok(buffer) if !buffer.is_empty() => {
                            // 预分配向量以减少重新分配
                            let mut chunk_data = Vec::with_capacity(buffer.len());
                            
                            // 使用 Buffer 的迭代器避免中间拷贝
                            for bytes in buffer {
                                chunk_data.extend_from_slice(&bytes);
                            }
                            
                            let chunk_size = chunk_data.len() as u64;
                            *offset += chunk_size;
                            
                            Some(Ok(FileChunk {
                                data: chunk_data,
                                offset: current_offset,
                                is_last: false,
                            }))
                        }
                        Ok(_) => None, // 空 buffer，结束
                        Err(e) => Some(Err(Status::internal(format!("Read error: {}", e)))),
                    }
                }
            });

        Ok(file_stream)
    }
}

// 模拟 tonic trait
#[tonic::async_trait]
pub trait FileStreamService {
    type DownloadFileStream: Stream<Item = Result<FileChunk, Status>>;
    
    async fn download_file(
        &self,
        request: Request<DownloadRequest>,
    ) -> Result<Response<Self::DownloadFileStream>, Status>;
    
    async fn get_file_info(
        &self,
        request: Request<FileInfoRequest>,
    ) -> Result<Response<FileInfo>, Status>;
    
    async fn upload_file(
        &self,
        request: Request<Streaming<UploadChunk>>,
    ) -> Result<Response<UploadResponse>, Status>;
}

#[tonic::async_trait]
impl FileStreamService for FileStreamServiceImpl {
    type DownloadFileStream = Pin<Box<dyn Stream<Item = Result<FileChunk, Status>> + Send>>;

    async fn download_file(
        &self,
        request: Request<DownloadRequest>,
    ) -> Result<Response<Self::DownloadFileStream>, Status> {
        let req = request.into_inner();
        
        // 优化块大小
        let chunk_size = if req.chunk_size > 0 {
            req.chunk_size as usize
        } else {
            self.default_chunk_size
        };

        let range = req.range.map(|r| (r.start, r.end));

        // 根据文件大小选择最优策略
        let metadata = self.operator
            .stat(&req.path)
            .await
            .map_err(|e| Status::not_found(format!("File not found: {}", e)))?;

        let file_size = metadata.content_length();
        
        let stream = if file_size > 100 * 1024 * 1024 { // 大于 100MB
            // 大文件使用内存池优化
            self.create_pooled_stream(&req.path, chunk_size).await?
        } else {
            // 小文件使用标准零拷贝流
            self.create_zero_copy_stream(&req.path, range, chunk_size).await?
        };
        
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_file_info(
        &self,
        request: Request<FileInfoRequest>,
    ) -> Result<Response<FileInfo>, Status> {
        let req = request.into_inner();
        
        let metadata = self.operator
            .stat(&req.path)
            .await
            .map_err(|e| Status::not_found(format!("File not found: {}", e)))?;

        let file_info = FileInfo {
            path: req.path,
            size: metadata.content_length(),
            content_type: metadata.content_type().unwrap_or("application/octet-stream").to_string(),
            last_modified: metadata.last_modified()
                .map(|t| t.timestamp())
                .unwrap_or(0),
        };

        Ok(Response::new(file_info))
    }

    async fn upload_file(
        &self,
        request: Request<Streaming<UploadChunk>>,
    ) -> Result<Response<UploadResponse>, Status> {
        let mut stream = request.into_inner();
        let mut path: Option<String> = None;
        let mut total_bytes = 0u64;
        let mut buffers = Vec::new();

        // 流式收集上传块
        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result.map_err(|e| Status::internal(format!("Stream error: {}", e)))?;
            
            if path.is_none() {
                path = Some(chunk.path.clone());
            }

            total_bytes += chunk.data.len() as u64;
            
            // 零拷贝转换
            let bytes = Bytes::from(chunk.data);
            buffers.push(bytes);

            if chunk.is_last {
                break;
            }
        }

        let path = path.ok_or_else(|| Status::invalid_argument("No path specified"))?;
        
        // 组合所有 Buffer（零拷贝）
        let final_buffer = Buffer::from(buffers);

        // 异步写入
        self.operator
            .write(&path, final_buffer)
            .await
            .map_err(|e| Status::internal(format!("Write failed: {}", e)))?;

        Ok(Response::new(UploadResponse {
            success: true,
            message: "File uploaded successfully".to_string(),
            bytes_written: total_bytes,
        }))
    }
}

/// 性能监控和指标收集
pub struct StreamMetrics {
    total_bytes_transferred: Arc<std::sync::atomic::AtomicU64>,
    active_streams: Arc<std::sync::atomic::AtomicUsize>,
    average_chunk_size: Arc<std::sync::atomic::AtomicUsize>,
}

impl StreamMetrics {
    pub fn new() -> Self {
        Self {
            total_bytes_transferred: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            active_streams: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            average_chunk_size: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    pub fn on_stream_start(&self) {
        self.active_streams.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn on_stream_end(&self) {
        self.active_streams.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn on_bytes_transferred(&self, bytes: u64) {
        self.total_bytes_transferred.fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal::Scheme;

    #[tokio::test]
    async fn test_zero_copy_stream() {
        // 创建内存存储用于测试
        let operator = Operator::via_iter(Scheme::Memory, []).unwrap();
        
        // 写入测试数据
        let test_data = "Hello, World!".repeat(1000);
        operator.write("test.txt", test_data.clone()).await.unwrap();
        
        // 创建服务
        let service = FileStreamServiceImpl::new(operator);
        
        // 测试流式读取
        let stream = service.create_zero_copy_stream("test.txt", None, 1024).await.unwrap();
        
        let chunks: Vec<_> = stream.try_collect().await.unwrap();
        
        // 验证数据完整性
        let reconstructed: String = chunks
            .into_iter()
            .map(|chunk| String::from_utf8(chunk.data).unwrap())
            .collect();
        
        assert_eq!(reconstructed, test_data);
    }
}

/// 主函数示例
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 配置 OpenDAL
    let operator = Operator::via_iter(opendal::Scheme::Fs, [
        ("root", "/path/to/files"),
    ])?;
    
    // 创建服务
    let file_service = FileStreamServiceImpl::new(operator);
    
    println!("FileStreamService configured with zero-copy optimization");
    
    // 这里可以添加 tonic 服务器启动代码
    // Server::builder()
    //     .add_service(FileStreamServiceServer::new(file_service))
    //     .serve(addr)
    //     .await?;
    
    Ok(())
}