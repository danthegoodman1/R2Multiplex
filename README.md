# R2 Multiplex

Example multiplexing R2 buckets (S3 API) to get around rate limits.

R2 Buckets have undocumented rate limits of around 400rps. This is due to the fact that they are backed by Durable Objects, so this limit is unlikely to raise soon.

The common suggestion is to multiplex bucket operations, but that can be really annoying.

This proxy worker verifies incoming requests, hashes the key to determine the target bucket, re-signs the request, and forwards it to the bucket.

To test, run the server first `npm run dev` then `npx vitest`

## Features

- **Automatic Key Distribution**: Uses consistent hashing to distribute objects across multiple buckets
- **Request Verification**: Validates AWS Signature v4 authentication before forwarding requests
- **Full ListObjectsV2 Support**: Automatically orchestrates list operations across all buckets and synthesizes proper API-compatible responses

## ListObjectsV2 API Support

The major enhancement is full ListObjectsV2 API compatibility. When a ListObjectsV2 request is received (`?list-type=2`), the worker:

1. **Queries All Buckets**: Sends parallel requests to all configured buckets
2. **Merges Results**: Combines objects from all buckets into a single sorted list
3. **Handles Pagination**: Supports continuation tokens for proper pagination across bucket boundaries
4. **Full Parameter Support**: Supports all standard ListObjectsV2 parameters:
   - `prefix` - Filter objects by prefix
   - `delimiter` - Group objects by delimiter (e.g., `/` for folder-like behavior)
   - `max-keys` - Limit number of results returned
   - `continuation-token` - For paginated results
   - `start-after` - Start listing after a specific key
   - `fetch-owner` - Include object owner information

### Example ListObjectsV2 Usage

```bash
# List all objects
GET /?list-type=2

# List with prefix filter
GET /?list-type=2&prefix=documents/

# List with delimiter for folder-like grouping
GET /?list-type=2&delimiter=/

# Paginated listing
GET /?list-type=2&max-keys=100&continuation-token=<token>
```

The response includes all standard S3 elements:
- `Contents` - Array of objects with Key, LastModified, ETag, Size, StorageClass
- `CommonPrefixes` - When using delimiter, groups common prefixes
- `KeyCount` - Number of objects in this response
- `IsTruncated` - Whether more results are available
- `NextContinuationToken` - Token for next page of results

## Configuration

Configure the bucket names in the `buckets` array and set up your environment variables for R2 access.
