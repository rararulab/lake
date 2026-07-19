const LOCAL_NO_PROXY = "localhost,127.0.0.1,::1,gateway.localhost";

export function icebergIntegrationEnvironment(
  ambient: Record<string, string | undefined>,
  restEndpoint: string,
  warehouse: string,
  s3Endpoint: string,
): Record<string, string | undefined> {
  const child = { ...ambient };
  for (const name of Object.keys(child)) {
    if (name.startsWith("AWS_") || name.startsWith("LAKE_ICEBERG_TEST_")) delete child[name];
  }

  return {
    ...child,
    LAKE_ICEBERG_TEST_REST_ENDPOINT: restEndpoint,
    LAKE_ICEBERG_TEST_WAREHOUSE: warehouse,
    LAKE_ICEBERG_TEST_S3_ENDPOINT: s3Endpoint,
    AWS_EC2_METADATA_DISABLED: "true",
    NO_PROXY: LOCAL_NO_PROXY,
    no_proxy: LOCAL_NO_PROXY,
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
