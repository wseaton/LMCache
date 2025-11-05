use anyhow::{Context, Result};
use aws_sdk_s3::Client;
use aws_config::BehaviorVersion;

/// configuration for S3 client
#[derive(Debug, Clone)]
pub struct S3ClientConfig {
    #[allow(dead_code)]
    pub endpoint: String,
    pub bucket: String,
    pub region: Option<String>,
    pub prefix: Option<String>,
    pub max_concurrency: usize,
}

impl S3ClientConfig {
    /// parse S3 endpoint URL (format: s3://bucket.endpoint or s3://bucket)
    pub fn from_url(url: &str) -> Result<Self> {
        if !url.starts_with("s3://") {
            anyhow::bail!("S3 URL must start with 's3://'");
        }

        let stripped = url.strip_prefix("s3://").unwrap();

        // parse bucket and endpoint
        // formats supported:
        // - s3://bucket.s3.region.amazonaws.com
        // - s3://bucket.s3express-zone_id.region.amazonaws.com
        // - s3://bucket (uses default endpoint)

        let parts: Vec<&str> = stripped.split('.').collect();

        if parts.is_empty() {
            anyhow::bail!("Invalid S3 URL format");
        }

        let bucket = parts[0].to_string();
        let endpoint = stripped.to_string();

        // extract region if present
        let region = if parts.len() >= 3 {
            Some(parts[parts.len() - 2].to_string())
        } else {
            None
        };

        Ok(Self {
            endpoint,
            bucket,
            region,
            prefix: None,
            max_concurrency: 64,
        })
    }

    pub fn with_prefix(mut self, prefix: Option<String>) -> Self {
        self.prefix = prefix;
        self
    }

    pub fn with_max_concurrency(mut self, max_concurrency: usize) -> Self {
        self.max_concurrency = max_concurrency;
        self
    }
}

/// wrapper around AWS S3 client
pub struct S3ClientWrapper {
    client: Client,
    config: S3ClientConfig,
}

impl S3ClientWrapper {
    /// create new S3 client
    pub async fn new(config: S3ClientConfig) -> Result<Self> {
        // load AWS config from environment
        let mut aws_config_builder = aws_config::defaults(BehaviorVersion::latest());

        if let Some(ref region) = config.region {
            aws_config_builder = aws_config_builder.region(
                aws_sdk_s3::config::Region::new(region.clone())
            );
        }

        let aws_config = aws_config_builder.load().await;

        // build S3 client config with endpoint override if available
        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);

        // check for AWS_ENDPOINT_URL environment variable (for MinIO/LocalStack)
        if let Ok(endpoint_url) = std::env::var("AWS_ENDPOINT_URL") {
            s3_config_builder = s3_config_builder.endpoint_url(endpoint_url);
        }

        // force path-style addressing for MinIO compatibility
        s3_config_builder = s3_config_builder.force_path_style(true);

        let s3_config = s3_config_builder.build();
        let client = Client::from_conf(s3_config);

        Ok(Self { client, config })
    }

    /// generate S3 key with optional prefix
    pub fn format_key(&self, key: &str) -> String {
        // flatten key (replace / with _)
        let flat_key = key.replace('/', "_");

        if let Some(prefix) = &self.config.prefix {
            format!("{}/{}", prefix, flat_key)
        } else {
            flat_key
        }
    }

    /// check if object exists (HEAD request)
    #[allow(dead_code)]
    pub async fn exists(&self, key: &str) -> Result<bool> {
        let s3_key = self.format_key(key);

        match self.client
            .head_object()
            .bucket(&self.config.bucket)
            .key(&s3_key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                let error_str = format!("{:?}", e);  // use Debug format for more detail
                // check for various not-found error patterns
                if error_str.contains("404")
                    || error_str.contains("NotFound")
                    || error_str.contains("NoSuchKey")
                    || error_str.contains("not found")
                    || error_str.contains("NoSuchBucket") {
                    Ok(false)
                } else {
                    // include full error for debugging
                    Err(anyhow::anyhow!("HEAD request failed: {:?}", e))
                }
            }
        }
    }

    /// get object size
    pub async fn get_object_size(&self, key: &str) -> Result<Option<usize>> {
        let s3_key = self.format_key(key);

        match self.client
            .head_object()
            .bucket(&self.config.bucket)
            .key(&s3_key)
            .send()
            .await
        {
            Ok(resp) => {
                let size = resp.content_length().unwrap_or(0) as usize;
                Ok(Some(size))
            }
            Err(e) => {
                let error_str = e.to_string();
                // check for various not-found error patterns
                if error_str.contains("404")
                    || error_str.contains("NotFound")
                    || error_str.contains("NoSuchKey")
                    || error_str.contains("not found") {
                    Ok(None)
                } else {
                    Err(anyhow::anyhow!("HEAD request failed: {}", e))
                }
            }
        }
    }

    /// download object directly to destination pointer (zero-copy streaming)
    pub async fn get_object(&self, key: &str, dst_ptr: usize, buffer_size: usize) -> Result<usize> {
        use futures::stream::TryStreamExt;

        let s3_key = self.format_key(key);

        let resp = self.client
            .get_object()
            .bucket(&self.config.bucket)
            .key(&s3_key)
            .send()
            .await
            .with_context(|| format!("Failed to GET object: {}", s3_key))?;

        let mut body = resp.body;
        let mut offset = 0;

        // stream directly to destination pointer - zero copy!
        while let Some(chunk) = body.try_next().await? {
            let chunk_len = chunk.len();
            if offset + chunk_len > buffer_size {
                anyhow::bail!("Buffer overflow: offset={}, chunk={}, buffer={}", offset, chunk_len, buffer_size);
            }

            unsafe {
                std::ptr::copy_nonoverlapping(
                    chunk.as_ptr(),
                    (dst_ptr + offset) as *mut u8,
                    chunk_len
                );
            }
            offset += chunk_len;
        }

        Ok(offset)
    }

    /// upload object from buffer
    pub async fn put_object(&self, key: &str, data: &[u8]) -> Result<()> {
        let s3_key = self.format_key(key);

        self.client
            .put_object()
            .bucket(&self.config.bucket)
            .key(&s3_key)
            .body(data.to_vec().into())
            .send()
            .await
            .with_context(|| format!("Failed to PUT object: {}", s3_key))?;

        Ok(())
    }

    /// upload object from owned buffer (zero-copy to ByteStream)
    pub async fn put_object_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
        let s3_key = self.format_key(key);

        self.client
            .put_object()
            .bucket(&self.config.bucket)
            .key(&s3_key)
            .body(data.into())
            .send()
            .await
            .with_context(|| format!("Failed to PUT object: {}", s3_key))?;

        Ok(())
    }

    /// get bucket name
    #[allow(dead_code)]
    pub fn bucket(&self) -> &str {
        &self.config.bucket
    }

    /// get endpoint
    #[allow(dead_code)]
    pub fn endpoint(&self) -> &str {
        &self.config.endpoint
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_url() {
        let url = "s3://my-bucket.s3.us-east-1.amazonaws.com";
        let config = S3ClientConfig::from_url(url).unwrap();

        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.region, Some("us-east-1".to_string()));
    }

    #[test]
    fn test_format_key() {
        let config = S3ClientConfig {
            endpoint: "bucket.s3.us-east-1.amazonaws.com".to_string(),
            bucket: "bucket".to_string(),
            region: Some("us-east-1".to_string()),
            prefix: Some("test_prefix".to_string()),
            max_concurrency: 64,
        };

        let wrapper = S3ClientWrapper {
            client: unsafe { std::mem::zeroed() }, // placeholder for test
            config,
        };

        let key = wrapper.format_key("my/key/path");
        assert_eq!(key, "test_prefix/my_key_path");
    }
}
