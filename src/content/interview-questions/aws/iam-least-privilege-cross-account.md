---
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
reviewedDate: "2026-10-16"
title: "How do you give a data pipeline least-privilege IAM access, including cross-account?"
seoTitle: "IAM Least Privilege and Cross-Account: Interview"
description: "IAM least privilege for a data pipeline: one role per job, scoped S3, Glue and KMS actions, and cross-account access with trust and bucket policies."
technology: ["aws"]
topic: ["iam", "security", "cross-account", "least-privilege"]
difficulty: "Medium"
questionType: ["scenario", "architecture"]
estimatedMinutes: 12
interviewRelevance: "High"
shortAnswer: "Give each job its own IAM role, assumed by the service that runs it (Glue, Lambda, EMR, Step Functions), never long-lived access keys. Grant only the actions and resources it needs: s3:GetObject on its input prefix, s3:PutObject on its output prefix, s3:ListBucket with a prefix condition, the specific Glue tables, and kms:Decrypt or GenerateDataKey on the one key. For cross-account access, two sides must both allow the call: the identity policy in the caller's account and a resource policy in the owner's account (bucket policy, KMS key policy), or the job assumes a role in the data account whose trust policy names the caller. Add guardrails with SCPs, RCPs and permission boundaries, and tighten with Access Analyzer and CloudTrail."
followUps: ["When would you assume a role in the other account instead of using a bucket policy?", "Why does cross-account S3 access fail with AccessDenied even though the bucket policy allows it?", "What is an external ID and when do you need one?", "How do you find the permissions a job actually uses?"]
related: ["articles:aws/iam-for-data-engineers", "articles:aws/s3-for-data-engineers", "articles:aws/lake-formation", "interview-questions:data-engineering/handle-pii-in-pipelines"]
versionContext: "Policy evaluation and cross-account rules checked against the IAM User Guide, the AWS Organizations guide (SCPs and RCPs) and the S3 User Guide (Object Ownership defaults) in October 2026. Policy documents were checked to parse as JSON and CLI commands were written from the documentation, not executed. The two-sided evaluation model runs on Python 3.11 and ignores condition keys, SCPs and boundaries."
sources:
  - { label: "IAM User Guide: Cross-account policy evaluation logic", url: "https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_evaluation-logic-cross-account.html" }
  - { label: "IAM User Guide: Security best practices in IAM", url: "https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html" }
  - { label: "AWS Organizations: Resource control policies (RCPs)", url: "https://docs.aws.amazon.com/organizations/latest/userguide/orgs_manage_policies_rcps.html" }
  - { label: "Amazon S3 User Guide: Controlling ownership of objects (Object Ownership)", url: "https://docs.aws.amazon.com/AmazonS3/latest/userguide/about-object-ownership.html" }
  - { label: "IAM Access Analyzer: Policy generation", url: "https://docs.aws.amazon.com/IAM/latest/UserGuide/access-analyzer-policy-generation.html" }
---

## Detailed explanation

### Least privilege inside one account

1. **One role per job or service**, not one "data-pipeline" role shared by everything. A compromised or buggy job can then only touch its own data, and CloudTrail shows which job did what.
2. **Trust policy**: only the service that runs the job can assume the role (`glue.amazonaws.com`, `lambda.amazonaws.com`, `states.amazonaws.com`), optionally with `aws:SourceAccount` or `aws:SourceArn` conditions against the confused-deputy problem.
3. **Permissions policy** scoped by action and resource:
   - S3: `GetObject` on the input prefix, `PutObject` (and `DeleteObject` only if the job overwrites) on the output prefix, `ListBucket` on the bucket with an `s3:prefix` condition.
   - Glue Data Catalog: `GetTable`, `GetPartitions`, `BatchCreatePartition` on the specific catalog, database and table ARNs.
   - KMS: `Decrypt` for input, `GenerateDataKey` for output, on the specific key ARNs. SSE-KMS objects are unreadable without them even when S3 allows the read.
   - Logs and metrics for its own log group.
4. **Guardrails** that cap what any policy can grant: **permission boundaries** on roles that developers create, **SCPs** limiting what principals in member accounts can do, and **RCPs** limiting who outside the organisation can reach your resources.
5. **Tighten from evidence**: IAM Access Analyzer can generate a policy from a role's CloudTrail activity, flag resources shared outside the account or organisation, and report unused permissions.

### Cross-account access

In a single account, an allow in either the identity policy or the resource policy is enough. **Across accounts, both sides must allow the request**: the caller's identity policy in the trusted account and the resource policy in the trusting (data-owning) account. Two patterns:

| Pattern | How | Use when |
|---|---|---|
| Resource policy | The bucket policy (and KMS key policy) in the data account names the pipeline role from the processing account; that role's own policy also allows the actions | Simple reads or writes of one bucket; the job keeps its own identity |
| Assume a role in the data account | The data account has a role whose trust policy names the processing role; the job calls `sts:AssumeRole` and uses the temporary credentials | Many resource types, or the data account wants to keep all permissions defined locally |

For writes into another account's bucket, keep **Object Ownership** set to *Bucket owner enforced* (the default for new buckets): ACLs are disabled and the bucket owner owns every object, so objects written cross-account do not end up unreadable by the owner.

## Example

Permissions for a Glue job that reads raw orders and writes curated orders, using one KMS key:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListOnlyOurPrefixes",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::acme-lake-prod",
      "Condition": {"StringLike": {"s3:prefix": ["raw/orders/*", "curated/orders/*"]}}
    },
    {
      "Sid": "ReadRaw",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::acme-lake-prod/raw/orders/*"
    },
    {
      "Sid": "WriteCurated",
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::acme-lake-prod/curated/orders/*"
    },
    {
      "Sid": "CatalogForOneTable",
      "Effect": "Allow",
      "Action": ["glue:GetTable", "glue:GetPartitions", "glue:BatchCreatePartition"],
      "Resource": [
        "arn:aws:glue:eu-west-1:111122223333:catalog",
        "arn:aws:glue:eu-west-1:111122223333:database/curated",
        "arn:aws:glue:eu-west-1:111122223333:table/curated/orders"
      ]
    },
    {
      "Sid": "UseLakeKey",
      "Effect": "Allow",
      "Action": ["kms:Decrypt", "kms:GenerateDataKey"],
      "Resource": "arn:aws:kms:eu-west-1:111122223333:key/1a2b3c4d-5678-90ab-cdef-111122223333"
    }
  ]
}
```

Cross-account read: the analytics account (444455556666) runs a job whose role reads the data account's bucket. The data account's bucket policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "AnalyticsJobReadsCurated",
    "Effect": "Allow",
    "Principal": {"AWS": "arn:aws:iam::444455556666:role/analytics-orders-job"},
    "Action": ["s3:GetObject"],
    "Resource": "arn:aws:s3:::acme-lake-prod/curated/orders/*"
  }]
}
```

The role in 444455556666 needs its own allow for `s3:GetObject` on that ARN, and, if the bucket uses SSE-KMS with a customer managed key, the key policy in the data account must allow that role `kms:Decrypt` and the role's policy must allow it too. The alternative, assuming a role in the data account:

<!-- noexec -->
```python
import boto3

sts = boto3.client("sts")
creds = sts.assume_role(
    RoleArn="arn:aws:iam::111122223333:role/orders-reader-for-analytics",
    RoleSessionName="analytics-orders-2026-10-15",
    DurationSeconds=3600,
)["Credentials"]

s3 = boto3.client(
    "s3",
    aws_access_key_id=creds["AccessKeyId"],
    aws_secret_access_key=creds["SecretAccessKey"],
    aws_session_token=creds["SessionToken"],
)
obj = s3.get_object(Bucket="acme-lake-prod", Key="curated/orders/dt=2026-10-15/part-00000.parquet")
```

The two-sided rule as a model: same-account access needs one allow, cross-account access needs both, and an explicit deny anywhere wins.

```python
def decide(caller_account, resource_account, identity_allows, resource_allows, explicit_deny=False):
    if explicit_deny:
        return "DENY (explicit)"
    if caller_account == resource_account:
        return "ALLOW" if (identity_allows or resource_allows) else "DENY (implicit)"
    return "ALLOW" if (identity_allows and resource_allows) else "DENY (implicit)"

cases = [
    ("same account, identity policy only", "111", "111", True, False),
    ("cross-account, bucket policy only", "444", "111", False, True),
    ("cross-account, identity policy only", "444", "111", True, False),
    ("cross-account, both allow", "444", "111", True, True),
]
for label, caller, owner, identity, resource in cases:
    print(f"{label:37} -> {decide(caller, owner, identity, resource)}")
```

```text
same account, identity policy only    -> ALLOW
cross-account, bucket policy only     -> DENY (implicit)
cross-account, identity policy only   -> DENY (implicit)
cross-account, both allow             -> ALLOW
```

## Trade-offs and pitfalls

- **`s3:*` on `*`** "to get it working" and never tightened. Start narrow and use Access Analyzer to see what is missing or unused.
- **Forgetting KMS**: S3 allows the read, the key policy does not, and the error still says AccessDenied.
- **`ListBucket` on the object ARN** (`bucket/*`) does nothing; it applies to the bucket ARN.
- **Account-wide principals** (`"Principal": {"AWS": "arn:aws:iam::444455556666:root"}`) in bucket policies delegate the decision to whatever the other account's admins grant; name the role, or add `aws:PrincipalOrgID` conditions.
- **Assume-role chains** limit sessions to one hour and complicate auditing; keep chains short.
- **Lake Formation-managed tables**: for registered locations, table access is granted in Lake Formation, and IAM S3 permissions alone do not give access to the data through Athena or Glue.

## Follow-up answers

- **Assume a role vs bucket policy**: assume a role when the job needs several resources in the other account (S3, Glue catalog, KMS, SQS) or the owner wants every permission defined and revocable in one place; a bucket policy is simpler for one bucket and keeps the caller's identity in the owner's CloudTrail logs.
- **AccessDenied despite the bucket policy**: the caller's identity policy does not allow the action, the KMS key policy or the role's KMS permissions are missing, an SCP, RCP, permission boundary or VPC endpoint policy denies it, or the object is owned by another account because ACLs were enabled.
- **External ID**: a value a third party must pass in `sts:AssumeRole`, checked with `sts:ExternalId` in the trust policy. It protects against a vendor being tricked into using your role on someone else's behalf (confused deputy); use it whenever a third-party SaaS assumes a role in your account.
- **Permissions actually used**: CloudTrail events for the role, IAM last-accessed information, and Access Analyzer policy generation and unused-access findings.
