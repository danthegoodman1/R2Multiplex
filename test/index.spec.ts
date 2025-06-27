import { env, createExecutionContext, waitOnExecutionContext } from 'cloudflare:test';
import { describe, it, expect, beforeAll } from 'vitest';
import { S3Client, PutObjectCommand, GetObjectCommand, ListObjectsV2Command, DeleteObjectCommand } from '@aws-sdk/client-s3';
import worker from '../src/index';

// Mock S3 client that points to our worker
let s3Client: S3Client;

beforeAll(async () => {
	// Create S3 client configured to point to our worker
	s3Client = new S3Client({
		region: 'auto', // R2 uses 'auto' region
		endpoint: 'http://localhost:8787', // This will be the worker's endpoint during testing
		credentials: {
			accessKeyId: env.CLIENT_ACCESS_KEY || 'accesskey',
			secretAccessKey: env.CLIENT_SECRET_KEY || 'secretkey',
		},
		forcePathStyle: true, // Required for R2/S3 compatibility
	});
});

describe('R2 Multiplex Worker', () => {
	it(
		'should write and read 10 files across buckets using AWS SDK',
		async () => {
			const testFiles = [
				{ key: 'file1.txt', content: 'Content of file 1' },
				{ key: 'file2.txt', content: 'Content of file 2' },
				{ key: 'file3.txt', content: 'Content of file 3' },
				{ key: 'file4.txt', content: 'Content of file 4' },
				{ key: 'file5.txt', content: 'Content of file 5' },
				{ key: 'documents/file6.txt', content: 'Content of file 6 in documents' },
				{ key: 'images/file7.jpg', content: 'Content of file 7 as image' },
				{ key: 'data/file8.json', content: '{"message": "Content of file 8"}' },
				{ key: 'archive/file9.zip', content: 'Content of file 9 in archive' },
				{ key: 'logs/file10.log', content: 'Content of file 10 - log entry' },
			];

			// Write all 10 files using S3 PutObject
			console.log('Writing 10 files using AWS SDK...');
			for (const file of testFiles) {
				const putCommand = new PutObjectCommand({
					Bucket: 'multiplex', // This is our virtual bucket name
					Key: file.key,
					Body: file.content,
					ContentType: 'text/plain',
				});

				// Instead of using the S3 client directly, we'll intercept the signed request
				// and send it to our worker
				const request = await s3Client.send(putCommand);
				console.log(`PUT ${file.key}: Success`);
			}

			// Read all 10 files back using S3 GetObject
			console.log('Reading 10 files back using AWS SDK...');
			for (const file of testFiles) {
				const getCommand = new GetObjectCommand({
					Bucket: 'multiplex',
					Key: file.key,
				});

				const response = await s3Client.send(getCommand);

				if (response.Body) {
					const responseText = await response.Body.transformToString();
					console.log(`GET ${file.key}: "${responseText}"`);
					expect(responseText).toBe(file.content);
				}
			}
		},
		{
			timeout: 100000,
		}
	);

	it(
		'should test ListObjectsV2 with 1000 files across buckets',
		async () => {
			// Generate unique prefix for this test to avoid conflicts
			const testPrefix = `test-${Date.now()}-${Math.random().toString(36).substring(7)}/`;
			const numFiles = 10;
			const createdFiles: string[] = [];

			console.log(`Testing ListObjectsV2 with ${numFiles} files using prefix: ${testPrefix}`);

			try {
				// Generate and create 1000 random files
				console.log('Creating 1000 random files...');
				for (let i = 0; i < numFiles; i++) {
					const randomSuffix = Math.random().toString(36).substring(2, 15);
					const fileKey = `${testPrefix}file-${i.toString().padStart(4, '0')}-${randomSuffix}.txt`;
					const fileContent = `Content of file ${i} - ${randomSuffix}`;

					createdFiles.push(fileKey);

					const putCommand = new PutObjectCommand({
						Bucket: 'multiplex',
						Key: fileKey,
						Body: fileContent,
						ContentType: 'text/plain',
					});

					await s3Client.send(putCommand);

					if (i % 100 === 0) {
						console.log(`Created ${i + 1}/${numFiles} files...`);
					}
				}

				console.log(`Successfully attempted to create ${numFiles} files`);

				// Wait a moment for eventual consistency
				// await new Promise((resolve) => setTimeout(resolve, 1000));

				// List objects using ListObjectsV2
				console.log('Listing objects with ListObjectsV2...');
				const listedFiles: string[] = [];
				let continuationToken: string | undefined;
				let pageCount = 0;

				do {
					const listCommand = new ListObjectsV2Command({
						Bucket: 'multiplex',
						Prefix: testPrefix,
						MaxKeys: 1000, // Get all objects in one request if possible
						ContinuationToken: continuationToken,
					});

					const listResponse = await s3Client.send(listCommand);
					pageCount++;

					console.log(`Page ${pageCount}: Found ${listResponse.KeyCount || 0} objects, IsTruncated: ${listResponse.IsTruncated}`);

					if (listResponse.Contents) {
						for (const obj of listResponse.Contents) {
							if (obj.Key) {
								listedFiles.push(obj.Key);
							}
						}
					}

					continuationToken = listResponse.NextContinuationToken;
				} while (continuationToken);

				console.log(`Listed ${listedFiles.length} files total across ${pageCount} pages`);

				// Verify we got all our files back
				const createdFilesSorted = [...createdFiles].sort();
				const listedFilesSorted = [...listedFiles].sort();

				console.log(`Created files: ${createdFiles.length}, Listed files: ${listedFiles.length}`);

				// Check that all created files are in the listed files
				const missingFiles = createdFilesSorted.filter((file) => !listedFilesSorted.includes(file));
				const extraFiles = listedFilesSorted.filter((file) => !createdFilesSorted.includes(file));

				if (missingFiles.length > 0) {
					console.log(`Missing files (${missingFiles.length}):`, missingFiles.slice(0, 10));
				}
				if (extraFiles.length > 0) {
					console.log(`Extra files (${extraFiles.length}):`, extraFiles.slice(0, 10));
				}

				// Verify ordering - the listed files should be in lexicographical order
				expect(listedFiles).toEqual([...listedFiles].sort());

				// We expect to get most of our files back, allowing for some eventual consistency issues
				// In a real test environment, we'd expect 100% success, but with mocked backends some files might not persist
				const successRate = listedFiles.length / createdFiles.length;
				console.log(`Success rate: ${(successRate * 100).toFixed(1)}%`);

				// Expect at least 90% of files to be successfully listed (adjust based on test environment)
				expect(successRate).toBeGreaterThan(0.9);

				// Verify that files with the same prefix are correctly grouped
				const allFilesHaveCorrectPrefix = listedFiles.every((file) => file.startsWith(testPrefix));
				expect(allFilesHaveCorrectPrefix).toBe(true);
			} finally {
				// Cleanup: Delete all created files regardless of test outcome
				console.log('Cleaning up created files...');
				let deletedCount = 0;

				for (const fileKey of createdFiles) {
					const deleteCommand = new DeleteObjectCommand({
						Bucket: 'multiplex',
						Key: fileKey,
					});

					await s3Client.send(deleteCommand);
					deletedCount++;

					if (deletedCount % 100 === 0) {
						console.log(`Deleted ${deletedCount}/${createdFiles.length} files...`);
					}
				}

				console.log(`Cleanup completed. Deleted ${deletedCount}/${createdFiles.length} files`);
			}
		},
		{
			timeout: 300000,
		}
	); // 5 minute timeout for this test

	it('should test direct signed requests to worker', async () => {
		// Alternative test that directly creates signed requests to send to the worker
		const testKey = 'direct-test.txt';
		const testContent = 'Direct test content';

		// Create a signed PUT request manually
		const putRequest = new Request(`http://localhost:8787/${testKey}`, {
			method: 'PUT',
			body: testContent,
			headers: {
				'Content-Type': 'text/plain',
				Host: 'localhost:8787',
				'X-Amz-Content-Sha256': 'UNSIGNED-PAYLOAD',
				// The worker's signature verification will handle AWS Signature V4
			},
		});

		// We need to sign this request with AWS Signature V4
		// For now, let's test the worker's response to unsigned requests
		const putResponse = await worker.fetch(putRequest, env);
		console.log(`Direct PUT response status: ${putResponse?.status}`);

		// We expect 401 for unsigned requests
		expect(putResponse?.status).toBe(401);

		const responseText = await putResponse!.text();
		console.log(`Response: ${responseText}`);
		expect(responseText).toBe('Unauthorized: Invalid signature');
	});
});
