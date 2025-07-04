import { SignatureV4 } from '@smithy/signature-v4';
import { Sha256 } from '@aws-crypto/sha256-js';
import { HttpRequest } from '@smithy/protocol-http';
import { XMLParser, XMLBuilder } from 'fast-xml-parser';

const buckets = ['aaaa', 'bbbb'];
const myBucket = 'multiplex';

function toHex(buffer: ArrayBuffer): string {
	return [...new Uint8Array(buffer)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

async function pickBucket(key: string): Promise<string> {
	const encoder = new TextEncoder();
	const data = encoder.encode(key);
	const hashBuffer = await crypto.subtle.digest('SHA-256', data);

	// Convert first 4 bytes of hash to a number for modulo
	const hashArray = new Uint8Array(hashBuffer);
	const hashNumber = (hashArray[0] << 24) | (hashArray[1] << 16) | (hashArray[2] << 8) | hashArray[3];
	const index = Math.abs(hashNumber) % buckets.length;

	return buckets[index];
}

// Parse Authorization header to extract signature components
function parseAuthorizationHeader(authHeader: string) {
	const match = authHeader.match(/^AWS4-HMAC-SHA256 Credential=([^,]+),\s*SignedHeaders=([^,]+),\s*Signature=(.+)$/);
	if (!match) {
		throw new Error('Invalid Authorization header format');
	}

	const [, credential, signedHeaders, signature] = match;
	const [accessKeyId, ...credentialParts] = credential.split('/');
	const [dateStamp, region, service, terminationString] = credentialParts;

	return {
		accessKeyId,
		dateStamp,
		region,
		service,
		terminationString,
		signedHeaders: signedHeaders.split(';'),
		signature,
	};
}

// Verify the incoming request signature
async function verifySignature(req: Request, env: Env, bodyContent?: ArrayBuffer): Promise<boolean> {
	try {
		const authHeader = req.headers.get('authorization');
		if (!authHeader || !authHeader.startsWith('AWS4-HMAC-SHA256')) {
			console.log('No valid authorization header found');
			return false;
		}

		const parsedAuth = parseAuthorizationHeader(authHeader);

		// Check if the access key matches our expected client key
		if (parsedAuth.accessKeyId !== env.CLIENT_ACCESS_KEY) {
			console.log(`Access key mismatch: ${parsedAuth.accessKeyId} !== ${env.CLIENT_ACCESS_KEY}`);
			return false;
		}

		// Create a signer with the client credentials to verify the signature
		const verifier = new SignatureV4({
			credentials: {
				accessKeyId: env.CLIENT_ACCESS_KEY,
				secretAccessKey: env.CLIENT_SECRET_KEY,
			},
			service: parsedAuth.service,
			region: parsedAuth.region,
			sha256: Sha256,
		});

		const url = new URL(req.url);

		// Create HttpRequest for signature verification - only include signed headers
		const signedHeadersOnly: Record<string, string> = {};
		for (const headerName of parsedAuth.signedHeaders) {
			const headerValue = req.headers.get(headerName);
			if (headerValue !== null) {
				signedHeadersOnly[headerName] = headerValue;
			}
		}

		const httpRequest = new HttpRequest({
			method: req.method,
			headers: signedHeadersOnly,
			hostname: url.hostname,
			path: url.pathname,
			query: url.search ? Object.fromEntries(url.searchParams) : undefined,
			body: bodyContent,
			protocol: url.protocol,
		});

		// Sign the request with our client credentials
		const expectedSigned = await verifier.sign(httpRequest);

		// Extract signature from expected signed request
		const expectedAuthHeader = expectedSigned.headers?.['authorization'] as string;
		if (!expectedAuthHeader) {
			console.log('Failed to generate expected signature');
			return false;
		}

		const expectedParsed = parseAuthorizationHeader(expectedAuthHeader);

		// Compare signatures
		const signaturesMatch = parsedAuth.signature === expectedParsed.signature;
		console.log('Signatures match:', signaturesMatch);
		return signaturesMatch;
	} catch (error) {
		console.error('Signature verification failed:', error);
		return false;
	}
}

// Types for ListObjectsV2 response structure
interface S3Object {
	Key: string;
	LastModified: string;
	ETag: string;
	Size: number;
	StorageClass: string;
	Owner?: {
		ID: string;
		DisplayName: string;
	};
}

interface CommonPrefix {
	Prefix: string;
}

interface ListObjectsV2Response {
	Name: string;
	Prefix?: string;
	KeyCount: number;
	MaxKeys: number;
	IsTruncated: boolean;
	Contents?: S3Object[];
	CommonPrefixes?: CommonPrefix[];
	ContinuationToken?: string;
	NextContinuationToken?: string;
	StartAfter?: string;
	Delimiter?: string;
}

// Create a signed request to R2
async function createSignedR2Request(
	method: string,
	bucketName: string,
	path: string,
	env: Env,
	queryParams?: URLSearchParams,
	headers?: Headers,
	body?: ArrayBuffer
): Promise<Request> {
	const targetUrl = new URL(`https://${env.ACCOUNT_ID}.r2.cloudflarestorage.com/${bucketName}${path}`);
	if (queryParams) {
		targetUrl.search = queryParams.toString();
	}

	const r2Headers = new Headers();

	// Filter headers to exclude Cloudflare-specific ones that get modified during forwarding
	if (headers) {
		for (const [key, value] of headers.entries()) {
			const lowerKey = key.toLowerCase();
			// Skip headers that Cloudflare modifies or adds when forwarding
			if (lowerKey.startsWith('cf-') || lowerKey === 'x-forwarded-for' || lowerKey === 'x-real-ip' || lowerKey === 'host') {
				continue;
			}
			r2Headers.set(key, value);
		}
	}

	r2Headers.set('host', targetUrl.host);

	// Calculate body hash for the signature
	const hash = new Sha256();
	hash.update(body || new Uint8Array());
	const bodyHash = toHex(await hash.digest());
	r2Headers.set('x-amz-content-sha256', bodyHash);

	const signer = new SignatureV4({
		credentials: { accessKeyId: env.R2_KEY, secretAccessKey: env.R2_SECRET },
		service: 's3',
		region: 'auto',
		sha256: Sha256,
	});

	const signed = await signer.sign(
		new HttpRequest({
			method,
			headers: Object.fromEntries(r2Headers.entries()),
			hostname: targetUrl.hostname,
			path: targetUrl.pathname,
			query: queryParams ? Object.fromEntries(queryParams.entries()) : undefined,
			body,
			protocol: 'https:',
		})
	);

	return new Request(targetUrl.toString(), {
		method: signed.method,
		headers: signed.headers,
		body,
	});
}

// Handle ListObjectsV2 requests by orchestrating across all buckets
async function handleListObjectsV2(req: Request, env: Env): Promise<Response> {
	const url = new URL(req.url);
	const params = url.searchParams;

	// Extract ListObjectsV2 parameters
	const prefix = params.get('prefix') || '';
	const delimiter = params.get('delimiter');
	const maxKeys = parseInt(params.get('max-keys') || '1000');
	const continuationToken = params.get('continuation-token');
	const startAfter = params.get('start-after');
	const fetchOwner = params.get('fetch-owner') === 'true';

	// Parse continuation token if present
	let parsedToken: { bucket: string; key: string; position: number } | null = null;
	if (continuationToken) {
		try {
			parsedToken = JSON.parse(atob(continuationToken));
		} catch (error) {
			return new Response('Invalid continuation token', { status: 400 });
		}
	}

	// Create requests to all buckets
	const bucketRequests = buckets.map(async (bucketName) => {
		const bucketParams = new URLSearchParams();
		bucketParams.set('list-type', '2');
		if (prefix) bucketParams.set('prefix', prefix);
		if (delimiter) bucketParams.set('delimiter', delimiter);
		bucketParams.set('max-keys', '1000'); // Get more from each bucket to ensure proper sorting
		if (startAfter) bucketParams.set('start-after', startAfter);
		if (fetchOwner) bucketParams.set('fetch-owner', 'true');

		// Handle continuation for this specific bucket
		if (parsedToken && parsedToken.bucket === bucketName) {
			bucketParams.set('start-after', parsedToken.key);
		}

		const signedRequest = await createSignedR2Request('GET', bucketName, '/', env, bucketParams);
		console.log(`ListObjectsV2 request to bucket ${bucketName}:`, signedRequest.url);

		const response = await fetch(signedRequest);

		if (!response.ok) {
			console.error(`ListObjectsV2 failed for bucket ${bucketName}:`, response.status, response.statusText);
			const errorBody = await response.text();
			console.error('ListObjectsV2 Error Body:', errorBody);
			throw new Error(`Failed to list objects in bucket ${bucketName}: ${response.statusText}`);
		}

		const xmlText = await response.text();
		const parser = new XMLParser({
			ignoreAttributes: false,
			parseAttributeValue: true,
		});

		return {
			bucketName,
			data: parser.parse(xmlText) as { ListBucketResult: ListObjectsV2Response },
		};
	});

	// Wait for all bucket requests to complete
	const bucketResults = await Promise.all(bucketRequests);

	// Merge and process results
	const allObjects: S3Object[] = [];
	const allCommonPrefixes: Set<string> = new Set();

	for (const result of bucketResults) {
		const bucketResult = result.data.ListBucketResult;

		if (bucketResult.Contents) {
			const contents = Array.isArray(bucketResult.Contents) ? bucketResult.Contents : [bucketResult.Contents];
			allObjects.push(...contents);
		}

		if (bucketResult.CommonPrefixes) {
			const prefixes = Array.isArray(bucketResult.CommonPrefixes) ? bucketResult.CommonPrefixes : [bucketResult.CommonPrefixes];
			prefixes.forEach((cp) => allCommonPrefixes.add(cp.Prefix));
		}
	}

	// Sort objects lexicographically by key (as per S3 API)
	allObjects.sort((a, b) => a.Key.localeCompare(b.Key));

	// Handle pagination - find starting position
	let startIndex = 0;
	if (parsedToken) {
		startIndex = allObjects.findIndex((obj) => obj.Key > parsedToken!.key);
		if (startIndex === -1) startIndex = allObjects.length;
	} else if (startAfter) {
		startIndex = allObjects.findIndex((obj) => obj.Key > startAfter);
		if (startIndex === -1) startIndex = allObjects.length;
	}

	// Slice to maxKeys limit
	const resultObjects = allObjects.slice(startIndex, startIndex + maxKeys);
	const isTruncated = startIndex + maxKeys < allObjects.length;

	// Generate next continuation token if needed
	let nextContinuationToken: string | undefined;
	if (isTruncated && resultObjects.length > 0) {
		const lastKey = resultObjects[resultObjects.length - 1].Key;
		const lastBucket = await pickBucket(lastKey);
		const token = {
			bucket: lastBucket,
			key: lastKey,
			position: startIndex + resultObjects.length,
		};
		nextContinuationToken = btoa(JSON.stringify(token));
	}

	// Build response
	const response: ListObjectsV2Response = {
		Name: myBucket,
		KeyCount: resultObjects.length,
		MaxKeys: maxKeys,
		IsTruncated: isTruncated,
		Contents: resultObjects.length > 0 ? resultObjects : undefined,
	};

	if (prefix) response.Prefix = prefix;
	if (delimiter) response.Delimiter = delimiter;
	if (continuationToken) response.ContinuationToken = continuationToken;
	if (nextContinuationToken) response.NextContinuationToken = nextContinuationToken;
	if (startAfter) response.StartAfter = startAfter;
	if (allCommonPrefixes.size > 0) {
		response.CommonPrefixes = Array.from(allCommonPrefixes)
			.sort()
			.map((prefix) => ({ Prefix: prefix }));
	}

	// Convert to XML
	const builder = new XMLBuilder({
		ignoreAttributes: false,
		format: true,
	});

	const xmlResponse = builder.build({
		ListBucketResult: {
			'@_xmlns': 'http://s3.amazonaws.com/doc/2006-03-01/',
			...response,
		},
	});

	return new Response(xmlResponse, {
		headers: {
			'Content-Type': 'application/xml',
			'x-amz-request-id': crypto.randomUUID(),
		},
		status: 200,
	});
}

export default {
	async fetch(req: Request, env: Env) {
		// Get the body content first (if any) so we can use it in both verification and forwarding
		let bodyContent: ArrayBuffer | undefined;

		if (req.body && (req.method === 'PUT' || req.method === 'POST' || req.method === 'PATCH')) {
			bodyContent = await req.arrayBuffer();
		}

		const isValidSignature = await verifySignature(req, env, bodyContent);
		if (!isValidSignature) {
			return new Response('Unauthorized: Invalid signature', { status: 401 });
		}

		const url = new URL(req.url);

		// Check for cross-bucket operations that need special handling
		if (req.method === 'GET') {
			// ListObjectsV2 - implemented
			if (url.searchParams.get('list-type') === '2') {
				return await handleListObjectsV2(req, env);
			}

			// ListObjects (v1) - not implemented yet
			if (url.searchParams.has('list-type') && url.searchParams.get('list-type') === '1') {
				return new Response('ListObjects v1 not implemented yet', { status: 501 });
			}

			// If no list-type specified, it's ListObjects v1 by default when no object key
			const pathWithoutBucket = url.pathname.replace(`/${myBucket}`, '').replace(/^\/+/, '');
			if (!pathWithoutBucket && !url.searchParams.has('list-type')) {
				return new Response('ListObjects v1 not implemented yet', { status: 501 });
			}

			// ListObjectVersions - not implemented yet
			if (url.searchParams.has('versions')) {
				return new Response('ListObjectVersions not implemented yet', { status: 501 });
			}

			// ListMultipartUploads - not implemented yet
			if (url.searchParams.has('uploads')) {
				return new Response('ListMultipartUploads not implemented yet', { status: 501 });
			}
		}

		// Extract the object key, handling bucket names in the path
		let key = url.pathname.slice(1); // Remove leading slash

		// If the path starts with a bucket name, remove it
		const pathParts = key.split('/');
		if (pathParts.length > 1 && pathParts[0] === myBucket) {
			console.log('removed bucket name from path');
			key = pathParts.slice(1).join('/');
		}

		console.log('Extracted key:', key);

		if (!key) {
			return new Response('Bad Request: No key specified', { status: 400 });
		}

		const bucket = await pickBucket(key);
		console.log('Selected bucket:', bucket, 'for key:', key);

		// Forward the request to the selected bucket
		const signedRequest = await createSignedR2Request(req.method, bucket, `/${key}`, env, url.searchParams, req.headers, bodyContent);

		// console.log('Forwarding to R2:', {
		// 	method: req.method,
		// 	bucket,
		// 	key,
		// 	targetUrl: signedRequest.url,
		// 	headers: Object.fromEntries(signedRequest.headers.entries()),
		// });

		const resp = await fetch(signedRequest);

		// console.log('R2 Response:', {
		// 	status: resp.status,
		// 	statusText: resp.statusText,
		// 	headers: Object.fromEntries(resp.headers.entries()),
		// });

		if (!resp.ok) {
			const errorBody = await resp.text();
			// console.log('R2 Error Body:', errorBody);
			// Return the R2 error response
			return new Response(errorBody, {
				status: resp.status,
				statusText: resp.statusText,
				headers: resp.headers,
			});
		}

		return resp;
	},
} satisfies ExportedHandler<Env>;
