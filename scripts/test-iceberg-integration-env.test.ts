import { expect, test } from "bun:test";

import { icebergIntegrationEnvironment, redactedEndpoint } from "./test-iceberg-integration-env";

test("Iceberg fixture environment removes ambient credentials and endpoints", () => {
  const child = icebergIntegrationEnvironment(
    {
      PATH: "/usr/bin",
      AWS_SESSION_TOKEN: "session-secret",
      AWS_PROFILE: "production-profile",
      LAKE_ICEBERG_TEST_REST_ENDPOINT: "https://wrong.invalid",
      LAKE_ICEBERG_TEST_S3_ENDPOINT: "https://wrong-objects.invalid",
    },
    "http://127.0.0.1:8181",
    "s3://icebergdata/demo",
    "http://127.0.0.1:9000",
  );

  expect(child.PATH).toBe("/usr/bin");
  expect(child.LAKE_ICEBERG_TEST_REST_ENDPOINT).toBe("http://127.0.0.1:8181");
  expect(child.LAKE_ICEBERG_TEST_WAREHOUSE).toBe("s3://icebergdata/demo");
  expect(child.LAKE_ICEBERG_TEST_S3_ENDPOINT).toBe("http://127.0.0.1:9000");
  expect(child.AWS_EC2_METADATA_DISABLED).toBe("true");
  expect(Object.values(child)).not.toContain("session-secret");
  expect(Object.values(child)).not.toContain("production-profile");
  expect(Object.values(child)).not.toContain("https://wrong.invalid");
});

test("Iceberg fixture endpoint logging omits credentials paths queries and fragments", () => {
  const endpoint = "https://user:password@catalog.invalid:8181/private?token=secret#fragment";

  expect(redactedEndpoint(endpoint)).toBe("https://catalog.invalid:8181");
  expect(redactedEndpoint("not a URL")).toBe("<redacted-invalid-endpoint>");
});
