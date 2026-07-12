export function integrationEnvironment(
  ambient: Record<string, string | undefined>,
  dynamoEndpoint: string,
  s3Endpoint: string,
): Record<string, string | undefined> {
  const child = { ...ambient };
  for (const name of Object.keys(child)) {
    if (name.startsWith("AWS_")) delete child[name];
  }

  return {
    ...child,
    LAKE_DYNAMODB_ENDPOINT: dynamoEndpoint,
    LAKE_S3_ENDPOINT: s3Endpoint,
    AWS_ACCESS_KEY_ID: "test",
    AWS_SECRET_ACCESS_KEY: "test",
    AWS_REGION: "us-east-1",
    AWS_DEFAULT_REGION: "us-east-1",
    AWS_EC2_METADATA_DISABLED: "true",
    LAKE_S3_PROXY_EXCLUDES: "localhost,127.0.0.1,::1",
    NO_PROXY: "localhost,127.0.0.1,::1",
    no_proxy: "localhost,127.0.0.1,::1",
  };
}

export function redactedEndpoint(endpoint: string): string {
  try {
    const origin = new URL(endpoint).origin;
    return origin === "null" ? "<redacted-invalid-endpoint>" : origin;
  } catch {
    return "<redacted-invalid-endpoint>";
  }
}
