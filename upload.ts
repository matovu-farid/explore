import {
  PutObjectCommand,
  S3Client,
  DeleteObjectCommand,
} from "@aws-sdk/client-s3";
import fs from "fs/promises";

const client = new S3Client({ region: "af-south-1" });

const data = await fs.readFile("./nodemodules.zip");

await client.send(
  new DeleteObjectCommand({
    Bucket: "explore-modules",
    Key: `nodemodules.zip`,
  })
);
await client.send(
  new PutObjectCommand({
    Bucket: "explore-modules",
    Key: `nodemodules.zip`,
    Body: data,
    ContentType: "text/plain",
  })
);
