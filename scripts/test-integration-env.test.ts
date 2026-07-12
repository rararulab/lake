import { expect, test } from "bun:test";

import { integrationEnvironment, redactedEndpoint } from "./test-integration-env";

test("integration environment removes every ambient AWS provider", () => {
  const ambient = {
    PATH: "/usr/bin",
    AWS_SESSION_TOKEN: "session-secret",
    AWS_PROFILE: "production-profile",
    AWS_SHARED_CREDENTIALS_FILE: "/secret/credentials",
    AWS_WEB_IDENTITY_TOKEN_FILE: "/secret/web-identity",
    AWS_CONTAINER_CREDENTIALS_FULL_URI: "http://credentials.invalid/?token=container-secret",
    AWS_CONTAINER_AUTHORIZATION_TOKEN: "authorization-secret",
    AWS_FUTURE_CREDENTIAL_PROVIDER: "future-secret",
  };

  const child = integrationEnvironment(
    ambient,
    "http://dynamodb.local:4566",
    "http://s3.local:4566",
  );

  expect(child.PATH).toBe("/usr/bin");
  expect(
    Object.keys(child)
      .filter((name) => name.startsWith("AWS_"))
      .sort(),
  ).toEqual([
    "AWS_ACCESS_KEY_ID",
    "AWS_DEFAULT_REGION",
    "AWS_EC2_METADATA_DISABLED",
    "AWS_REGION",
    "AWS_SECRET_ACCESS_KEY",
  ]);
  expect(Object.values(child)).not.toContain("session-secret");
  expect(Object.values(child)).not.toContain("production-profile");
  expect(Object.values(child)).not.toContain("future-secret");
});

test("endpoint logging omits credentials paths queries and fragments", () => {
  const endpoint = "https://user:password@localstack.invalid:4566/private?token=secret#fragment";

  expect(redactedEndpoint(endpoint)).toBe("https://localstack.invalid:4566");
  expect(redactedEndpoint("not a URL")).toBe("<redacted-invalid-endpoint>");
});
