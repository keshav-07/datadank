---
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
reviewedDate: "2026-10-16"
title: "Lake Formation permissions vs IAM and S3 bucket policies: how do they work together?"
seoTitle: "Lake Formation vs IAM and S3 Policies: Interview"
description: "Lake Formation vs IAM and S3 bucket policies: catalog vs data access, credential vending, IAMAllowedPrincipals, hybrid mode and row and column filters."
technology: ["aws"]
topic: ["lake-formation", "iam", "governance", "access-control"]
difficulty: "Hard"
questionType: ["conceptual", "scenario"]
estimatedMinutes: 12
interviewRelevance: "High"
shortAnswer: "IAM and bucket policies grant access to API actions and S3 objects; they cannot say \"these columns or rows of this table\". Lake Formation grants permissions on Data Catalog objects (databases, tables, columns, rows and cells through data filters, or by LF-Tags) and, for S3 locations registered with it, vends temporary credentials to integrated engines such as Athena, Redshift Spectrum, Glue and EMR, so users need no direct S3 access. A query then needs both: IAM permission to call the service and lakeformation:GetDataAccess, and a Lake Formation grant on the table. By default IAMAllowedPrincipals keeps IAM-only behaviour, so you must revoke it or opt principals in with hybrid access mode, and remove broad direct S3 permissions, or users can bypass the table rules."
followUps: ["What is IAMAllowedPrincipals and why do grants seem to have no effect?", "How do you share a governed table with another account?", "When would you use LF-Tags instead of named-resource grants?", "Can a user with s3:GetObject on the bucket still read a governed table's files?"]
related: ["articles:aws/lake-formation", "articles:aws/iam-for-data-engineers", "interview-questions:aws/iam-least-privilege-cross-account", "articles:databricks/unity-catalog-governance"]
versionContext: "Default settings (IAMAllowedPrincipals and Use only IAM access control), hybrid access mode, credential vending and the lakeformation:GetDataAccess requirement checked against the AWS Lake Formation Developer Guide in October 2026. CLI commands need an AWS account and were written from the documentation, not executed. The access model runs on Python 3.11 and is a simplification that ignores SCPs, KMS and cross-account sharing."
sources:
  - { label: "AWS Lake Formation: Underlying data access control", url: "https://docs.aws.amazon.com/lake-formation/latest/dg/access-control-underlying-data.html" }
  - { label: "AWS Lake Formation: Changing the default settings for your data lake", url: "https://docs.aws.amazon.com/lake-formation/latest/dg/change-settings.html" }
  - { label: "AWS Lake Formation: Hybrid access mode", url: "https://docs.aws.amazon.com/lake-formation/latest/dg/hybrid-access-mode.html" }
  - { label: "AWS Lake Formation: Data filtering and cell-level security", url: "https://docs.aws.amazon.com/lake-formation/latest/dg/data-filtering.html" }
  - { label: "AWS Lake Formation: Tag-based access control", url: "https://docs.aws.amazon.com/lake-formation/latest/dg/tag-based-access-control.html" }
  - { label: "AWS Lake Formation: Cross-account data sharing", url: "https://docs.aws.amazon.com/lake-formation/latest/dg/cross-account-permissions.html" }
---

## Detailed explanation

### What each layer controls

| Layer | Grants | Granularity | Enforced by |
|---|---|---|---|
| IAM identity policies | API actions (`athena:StartQueryExecution`, `glue:GetTable`, `s3:GetObject`) | Resource ARNs, conditions | Every AWS API call |
| S3 bucket policies | Object and bucket actions | Bucket and prefix | S3 |
| Lake Formation | `SELECT`, `INSERT`, `DELETE`, `DESCRIBE`, `ALTER`, `DROP` on catalogs, databases, tables; data location access | Columns, rows and cells (data filters), LF-Tag expressions | Integrated engines (Athena, Redshift Spectrum, Glue, EMR, EMR Serverless) and the Data Catalog |

### How a governed query works

1. An analyst's role runs an Athena query. IAM must allow the Athena and Glue calls and `lakeformation:GetDataAccess`.
2. Athena asks Lake Formation what the role may see on the table. Lake Formation checks its grants (named resources or LF-Tags) and any data filter.
3. For a **registered** S3 location, Lake Formation assumes the registration role and **vends temporary, scoped credentials** to Athena, which reads the files and returns only permitted columns and rows.

The analyst's role never needs `s3:GetObject` on the lake bucket. That is the point: if it had it, the person could read the Parquet files directly and skip column and row rules.

### Why grants "do nothing" at first

For backward compatibility, Lake Formation starts with **Super granted to `IAMAllowedPrincipals`** on existing catalog resources and "Use only IAM access control" on for new ones. Any principal IAM allows can then use the table, and Lake Formation grants are not in effect. Moving to Lake Formation means:

- Register the S3 location with a role Lake Formation can assume.
- Either revoke `IAMAllowedPrincipals` on the databases and tables (and turn off the default for new ones), or use **hybrid access mode**, where only **opted-in** principals are governed by Lake Formation and everyone else continues with IAM. Hybrid mode allows a gradual migration per team.
- Remove broad `s3:*` grants on lake buckets from human and BI roles.

### Scaling grants with LF-Tags

Named-resource grants (`GRANT SELECT ON sales.orders TO analysts`) grow with tables times roles. **LF-Tags** (for example `domain=sales`, `sensitivity=pii`) are attached to databases, tables or columns, and grants are made on tag expressions, so a new table tagged `domain=sales, sensitivity=internal` is immediately visible to the right roles.

## Example

Grant a column subset and a row filter on one table, and grant by tag for a whole domain:

<!-- noexec -->
```bash
# Register the lake location with a role Lake Formation will assume
aws lakeformation register-resource \
  --resource-arn arn:aws:s3:::acme-lake-curated \
  --role-arn arn:aws:iam::111122223333:role/lf-registration-curated

# Stop IAM-only access on this table
aws lakeformation revoke-permissions \
  --principal DataLakePrincipalIdentifier=IAM_ALLOWED_PRINCIPALS \
  --resource '{"Table": {"DatabaseName": "sales", "Name": "orders"}}' \
  --permissions ALL

# Row and column filter: EU analysts see EU rows, without the email column
aws lakeformation create-data-cells-filter --table-data '{
  "TableCatalogId": "111122223333", "DatabaseName": "sales", "TableName": "orders",
  "Name": "eu_no_email",
  "RowFilter": {"FilterExpression": "region = '\''EU'\''"},
  "ColumnWildcard": {"ExcludedColumnNames": ["customer_email"]}
}'

aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::111122223333:role/analyst-eu \
  --resource '{"DataCellsFilter": {"TableCatalogId": "111122223333", "DatabaseName": "sales",
               "TableName": "orders", "Name": "eu_no_email"}}' \
  --permissions SELECT

# Tag-based: data engineers can read every non-PII table in the sales domain
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::111122223333:role/sales-data-engineer \
  --resource '{"LFTagPolicy": {"ResourceType": "TABLE", "Expression": [
                 {"TagKey": "domain", "TagValues": ["sales"]},
                 {"TagKey": "sensitivity", "TagValues": ["internal", "public"]}]}}' \
  --permissions SELECT DESCRIBE
```

A simplified model of what each principal can do on a registered table, showing why direct S3 permissions must go:

```python
def via_athena(p):
    if not p["iam_athena_glue"] or not p["iam_get_data_access"]:
        return "denied by IAM"
    if p["iam_only_mode"]:                       # IAMAllowedPrincipals still has Super, or not opted in
        return "full table (IAM decides)"
    return p["lf_grant"] or "denied by Lake Formation"

def via_s3(p):
    return "reads every file, all columns" if p["direct_s3_read"] else "denied by S3"

principals = {
    "analyst-eu (governed)": dict(iam_athena_glue=True, iam_get_data_access=True, iam_only_mode=False,
                                  lf_grant="EU rows, no email", direct_s3_read=False),
    "legacy-bi (not opted in)": dict(iam_athena_glue=True, iam_get_data_access=True, iam_only_mode=True,
                                     lf_grant=None, direct_s3_read=True),
    "intern (no LF grant)": dict(iam_athena_glue=True, iam_get_data_access=True, iam_only_mode=False,
                                 lf_grant=None, direct_s3_read=False),
    "engineer with s3:* left over": dict(iam_athena_glue=True, iam_get_data_access=True, iam_only_mode=False,
                                         lf_grant="EU rows, no email", direct_s3_read=True),
}
for name, p in principals.items():
    print(f"{name:30} Athena: {via_athena(p):26} S3: {via_s3(p)}")
```

```text
analyst-eu (governed)          Athena: EU rows, no email          S3: denied by S3
legacy-bi (not opted in)       Athena: full table (IAM decides)   S3: reads every file, all columns
intern (no LF grant)           Athena: denied by Lake Formation   S3: denied by S3
engineer with s3:* left over   Athena: EU rows, no email          S3: reads every file, all columns
```

The last row is the common audit finding: the table rules are correct, but an old IAM policy lets the same person read the raw files.

## Trade-offs and pitfalls

- **Forgetting `IAMAllowedPrincipals`**: grants appear to work in testing (the tester is an admin) but everyone with IAM access still sees everything.
- **Leaving direct S3 access** on governed buckets for humans and BI roles. Keep it only for the registration role and pipeline roles that write data.
- **Engines that are not integrated** (a script with boto3 reading S3, a third-party tool without Lake Formation support) do not enforce column or row filters; they rely on S3 permissions alone.
- **Two sources of truth** during migration: hybrid mode helps, but document which principals are opted in and finish the migration.
- **Writers need Lake Formation too**: Glue jobs writing to governed tables need `INSERT`/`ALTER` grants and data location permissions, not just S3 `PutObject`.
- **KMS still applies**: the registration role needs the key permissions for SSE-KMS buckets.

## Follow-up answers

- **`IAMAllowedPrincipals`**: a virtual group meaning "anyone IAM allows". While it holds Super on a resource, Lake Formation effectively defers to IAM. Revoke it per database or table (or use hybrid mode with opted-in principals) to make grants effective.
- **Cross-account sharing**: grant the table or an LF-Tag expression to the other account; Lake Formation shares it through AWS RAM, the consumer account creates a resource link in its own catalog and its data lake admin grants access to local roles.
- **LF-Tags vs named grants**: tags when you have many tables and a classification scheme (domain, sensitivity) so new tables inherit access by tagging; named grants for a few exceptions.
- **`s3:GetObject` on the bucket**: yes, that principal can read the files directly through S3, bypassing Lake Formation. Governance requires removing that permission, using S3 Access Grants or bucket policies that allow only pipeline and registration roles.
