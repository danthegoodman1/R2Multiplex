import { SignatureV4 } from '@smithy/signature-v4';
import { Sha256 } from '@aws-crypto/sha256-js';
import { HttpRequest } from '@smithy/protocol-http';

const buckets = ['aaaa', 'bbbb'];
const myBucket = 'multiplex';

function toHex(buffer: ArrayBuffer): string {
	return [...new Uint8Array(buffer)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

function pickBucket(key: string): string {
	const sum = key.split('').reduce((acc, char) => acc + char.charCodeAt(0), 0);
	const index = sum % buckets.length;
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

		// console.log('Authorization header:', authHeader);
		const parsedAuth = parseAuthorizationHeader(authHeader);
		// console.log('Parsed auth:', parsedAuth);

		// Check if the access key matches our expected client key
		if (parsedAuth.accessKeyId !== env.CLIENT_ACCESS_KEY) {
			console.log(`Access key mismatch: ${parsedAuth.accessKeyId} !== ${env.CLIENT_ACCESS_KEY}`);
			return false;
		}

		// console.log('Access key matches, verifying signature...');

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

		// console.log('Request URL:', req.url);
		// console.log('Request method:', req.method);
		// console.log('Request headers:', Object.fromEntries(req.headers));

		// Create HttpRequest for signature verification - only include signed headers
		const signedHeadersOnly: Record<string, string> = {};
		for (const headerName of parsedAuth.signedHeaders) {
			const headerValue = req.headers.get(headerName);
			if (headerValue !== null) {
				signedHeadersOnly[headerName] = headerValue;
			}
		}

		// console.log('Signed headers only:', signedHeadersOnly);

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
		// console.log('Expected signature:', expectedParsed.signature);
		// console.log('Received signature:', parsedAuth.signature);

		// Compare signatures
		const signaturesMatch = parsedAuth.signature === expectedParsed.signature;
		console.log('Signatures match:', signaturesMatch);
		return signaturesMatch;
	} catch (error) {
		console.error('Signature verification failed:', error);
		return false;
	}
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

		// 1. Extract the object key, handling bucket names in the path
		// Path format: /bucket/key or /key (depending on how the client sends it)
		let key = url.pathname.slice(1); // Remove leading slash

		// If the path starts with a bucket name, remove it
		// since we'll determine the actual bucket through consistent hashing
		const pathParts = key.split('/');
		if (pathParts.length > 1 && pathParts[0] === myBucket) {
			// Remove the virtual bucket name, keep the rest as the key
			console.log('removed bucket name from path');
			key = pathParts.slice(1).join('/');
		}

		console.log('Extracted key:', key);

		if (!key) {
			return new Response('Bad Request: No key specified', { status: 400 });
		}

		const bucket = pickBucket(key); // your consistent-hash function
		console.log('Selected bucket:', bucket, 'for key:', key);
		const target = `https://${env.ACCOUNT_ID}.r2.cloudflarestorage.com/${bucket}/${key}`;
		const targetUrl = new URL(target);

		// Create a fresh set of headers for the R2 request. We can't just forward the original
		// request because the host and signature are different.
		const r2Headers = new Headers();

		// Copy over relevant non-auth headers from the original request
		const headersToCopy = ['content-type', 'content-encoding', 'content-disposition', 'cache-control'];
		for (const headerName of headersToCopy) {
			if (req.headers.has(headerName)) {
				r2Headers.set(headerName, req.headers.get(headerName)!);
			}
		}

		// Set required SigV4 headers
		r2Headers.set('host', targetUrl.host);

		// Calculate body hash for the signature
		const hash = new Sha256();
		hash.update(bodyContent || new Uint8Array());
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
				method: req.method,
				headers: Object.fromEntries(r2Headers.entries()),
				hostname: targetUrl.hostname,
				path: targetUrl.pathname,
				body: bodyContent,
				protocol: 'https:',
			})
		);

		// Forward the signed request to R2
		const resp = await fetch(target, {
			method: signed.method,
			headers: signed.headers,
			body: bodyContent,
		});
		return resp;
	},
} satisfies ExportedHandler<Env>;
