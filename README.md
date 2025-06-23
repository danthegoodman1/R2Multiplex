# R2 Multiplex

Example multiplexing R2 buckets (S3 API) to get around rate limits.

R2 Buckets have undocumented rate limits of around 400rps. This is due to the fact that they are backed by Durable Objects, so this limit is unlikely to raise soon.

The common suggestion is to multiplex bucket operations, but that can be really annoying.

This proxy worker verifies incoming requests, hashes the key to determine the target bucket, re-signs the request, and forwards it to the bucket.

Obviously listing operations break (but they could be reconstructed)
