# External API Documentation

## Authentication

All API requests require two headers:

```text
api-tenant: your_tenant_name
api-token: your_api_key
```

**Example:**

```bash
curl -H "api-tenant: your_tenant_name" \
     -H "api-token: your_api_key_here" \
     https://api.ediphi.com/api/external/projects
```

## Base URL

**Production:** `https://api.ediphi.com/api/external`

All endpoints are prefixed with `/api/external`.

**Important:** All tenants use the same API endpoint. Your tenant is identified by the
`api-tenant` header, not by the URL.

---

## Ediphi External API

### Projects

#### List Projects

```text
GET /projects
```

**Query Parameters (all optional):**

- `from` - Filter projects created on/after this date (ISO 8601). Works alone or with `to`.
- `to` - Filter projects created on/before this date (ISO 8601). Works alone or with `from`.
- `opportunity_number` - Filter by exact CRM opportunity number.
- `name` - Filter by project name (case-insensitive partial match).

**Pagination / sorting:** Not supported. This endpoint does **not** accept
`page` / `limit` / `order` / `dir` or the `search` filter — use the four parameters above.

**Example:**

```bash
curl -H "api-tenant: your_tenant_name" \
     -H "api-token: your_api_key_here" \
     "https://api.ediphi.com/api/external/projects?from=2024-01-01&to=2024-12-31"
```

**Response:**

```json
{
  "data": [
    {
      "project": {
        "id": "uuid",
        "name": "Project Name",
        "created_at": "2024-01-01T00:00:00Z"
      },
      "estimates": []
    }
  ]
}
```

#### Create Project

```text
POST /projects
```

**Request Body:**

```json
{
  "name": "New Project",
  "region": "region_id",
  "owner": "user_id",
  "opportunity": "opportunity_id",
  "industry": "industry_name",
  "phase": "ACTIVE"
}
```

**Field Notes:**

- `name` - Project name.
- `region`, `owner`, `opportunity` - Optional foreign keys. If provided, each must be a
  valid UUID that already exists in your tenant, otherwise the request returns 400.
- `industry` - Optional. Must be a valid industry name that exists in your tenant. Get
  valid industries from existing projects or contact support.
- `phase` - Optional. Valid phases: `IN_REVIEW`, `ACTIVE`, `ATTENTION`, `WAITING`.

An empty request body returns 400.

**Pagination / sorting:** Not applicable.

#### Update Project

```text
PUT /projects/:id
```

Updates an existing project. `:id` must be the UUID of an existing project (otherwise 400).

**Updatable fields (any others are ignored):** `name`, `owner`, `phase`, `industry`,
`region`, `client_name`, `address`, `task_number`, `opportunity`, `city`, `state`, `zip`.

The same validation as Create Project applies to `region` / `owner` / `opportunity`
(existing UUIDs), `industry` (existing name), and `phase` (valid value). Setting `address`
regenerates the project map image from `address` / `city` / `state`.

**Pagination / sorting:** Not applicable.

---

### Vendors

#### List Vendors

```text
GET /vendors
```

**Query Parameters:**

- `page` - Page number (default: 1)
- `limit` - Items per page (default: 100)
- `order` - Sort field (default: created_at)
- `dir` - Sort direction: asc or desc (default: desc)
- `search` - Filter, repeatable. Format `column;operator;value`. **Allowed columns:**
  `created_at`, `updated_at`. **Operators:** `>=`, `>`, `<`, `<=`. Value must be a date
  (`YYYY-MM-DD`).

**Pagination / sorting / filtering:** Fully supported (see Common Patterns).

**Example:**

```bash
curl -H "api-tenant: your_tenant_name" \
     -H "api-token: your_api_key_here" \
     "https://api.ediphi.com/api/external/vendors?page=1&limit=50&search=created_at;>=;2024-01-01"
```

#### Get Single Vendor

```text
GET /vendors/:id
```

Returns a single vendor by UUID. Not found returns 404. Pagination not applicable.

#### Batch Upsert Vendors

```text
PUT /vendors/batch
```

Create or update multiple vendors (max 200) and their contacts (max 200 total across all
vendors).

**Request Body:**

```json
{
  "vendors": [
    {
      "id": "existing_vendor_id",
      "contractor_name": "Vendor Name",
      "contacts": [
        {
          "id": "existing_contact_id",
          "first_name": "John",
          "last_name": "Doe",
          "email": "john@vendor.com",
          "mobile": "555-1234",
          "address_1": "123 Main St",
          "city": "San Francisco",
          "state": "CA",
          "zip": "94105",
          "regions": ["Region Name"],
          "bid_packages": ["03-CONCRETE"]
        }
      ]
    }
  ]
}
```

**Required Fields:**

- Vendor: `contractor_name` (omit `id` to create a new vendor).
- Contact: `first_name`, `last_name`, `email`, `mobile`, `address_1`, `city`, `state`,
  `zip` (omit `id` to create a new contact).

`regions` (names) and `bid_packages` (codes) on a contact are optional. Exceeding either
200 limit, or sending an empty / non-array `vendors`, returns 400. Response echoes the
processed vendors with their contacts under `{ "vendors": [] }`.

**Pagination / sorting:** Not applicable.

#### Batch Delete Vendors

```text
DELETE /vendors/batch
```

**Request Body:**

```json
{
  "vendor_ids": ["id1", "id2"],
  "vendor_contact_ids": ["id3", "id4"]
}
```

Provide **`vendor_ids`** and/or **`vendor_contact_ids`** (arrays of UUIDs). You can delete
contacts independently of vendors. At least one non-empty array is required, otherwise 400.
Returns `204 No Content` on success.

**Pagination / sorting:** Not applicable.

---

### Vendor Contacts

#### List Vendor Contacts

```text
GET /vendor_contacts
```

**Query Parameters:**

- `page` - Page number (default: 1)
- `limit` - Items per page (default: 100)
- `order` - Sort field (default: created_at)
- `dir` - Sort direction: asc or desc (default: desc)
- `search` - Filter, repeatable. Format `column;operator;value`. **Allowed columns:**
  `created_at`, `updated_at` (operators `>=`, `>`, `<`, `<=`, date value); `vendor`,
  `prequal_id` (operator `=`, UUID value). You may filter by `vendor` **or** `prequal_id`,
  but not both.

**Pagination / sorting / filtering:** Fully supported (see Common Patterns).

#### Get Single Vendor Contact

```text
GET /vendor_contacts/:id
```

Returns a single vendor contact by UUID. Not found returns 404. Pagination not applicable.

---

### Estimates

#### Get Estimate

```text
GET /estimates/:id
```

Returns detailed serialized estimate information for the estimate UUID. No query parameters.
Pagination not applicable.

---

### Users

#### List Users

```text
GET /users
```

Returns a list of users in your tenant (`id`, `full_name`, `email`, `admin`, `created_at`,
`updated_at`). No query parameters. Pagination not supported.

---

### Opportunities

#### List Opportunities

```text
GET /opportunities
```

Returns CRM opportunities linked to projects. No query parameters. Pagination not supported.

#### Sync CRM Data

```text
PUT /opportunities/sync
```

Synchronizes CRM opportunities with Ediphi. The request body is a JSON **array** of
opportunity objects (a non-array body returns 400).

**Recognized fields (case-insensitive):** `opty_number` (required per item — items without
it are skipped), `name`, `opty_creation_date`, `account_name`, `owner_name`,
`architectaccountname_c`, `project_address1`, `project_address2`, `project_city`,
`project_state`, `project_postal_code`. Include `"delete": true` on an item to remove that
opportunity.

**Request Body:**

```json
[
  {
    "opty_number": "OPTY-123",
    "name": "Opportunity Name",
    "account_name": "Account",
    "owner_name": "Owner",
    "project_city": "San Francisco",
    "project_state": "CA"
  }
]
```

**Pagination / sorting:** Not applicable.

#### Get Opportunity by Number

```text
GET /crm/:opportunityNumber
```

Retrieves a specific CRM opportunity (with its processed project/estimate data) by its
opportunity number. A missing number, or no project matching the opportunity, returns 400.
Pagination not applicable.

---

### Employees

#### Batch Upsert Employees (ERM)

```text
PUT /employees/erm_batch
```

Batch create or update employee records, typically used for ERM (Enterprise Resource
Management) integrations. The request body is a JSON **array** of ERM records (non-array
returns 400).

**Per-record fields:** `EMPLOYEE_NUMBER` (required — a record missing it returns 400),
`EMPLOYEE_NAME`, `EMPLOYEE_OFFICE_LOCATION` (matched to a region by name),
`EMPLOYEE_JOB_TITLE`.

**Note:** Before upserting the batch, any employee not updated today is marked inactive;
employees present in the payload are (re)activated.

**Pagination / sorting:** Not applicable.

---

## Common Patterns

### Pagination

Pagination is supported on **`GET /vendors`** and **`GET /vendor_contacts`** only. Other
list endpoints (`GET /projects`, `GET /users`, `GET /opportunities`) do not accept
pagination parameters — see each endpoint for its supported filters.

Where supported:

- `page` - Page number (starts at 1, default 1)
- `limit` - Items per page (default 100)
- `order` - Field to sort by (default `created_at`)
- `dir` - Sort direction: `asc` or `desc` (default `desc`)

### Date Filtering

Date values accept ISO 8601 format: `2024-01-01T00:00:00Z` or `2024-01-01`. `GET /projects`
uses dedicated `from` / `to` parameters; `GET /vendors` and `GET /vendor_contacts` filter
dates via the `search` parameter below.

### Search / Filtering

`GET /vendors` and `GET /vendor_contacts` support a repeatable `search` parameter. Each
`search` value has the form `column;operator;value`, and multiple `search` parameters are
combined:

```text
GET /vendors?search=created_at;>=;2024-01-01&search=created_at;<=;2024-12-31
```

- **Operators:** `>=`, `>`, `<`, `<=` for date columns; `=` for UUID columns.
- **Vendors — allowed columns:** `created_at`, `updated_at`.
- **Vendor Contacts — allowed columns:** `created_at`, `updated_at`, `vendor` (UUID),
  `prequal_id` (UUID). Use `vendor` or `prequal_id`, not both.

Invalid columns, operators, or values return 400.

---

## Data API (data.ediphi.com)

The Data API provides direct, read-only access to your tenant's reference and lookup tables
via native SQL. It is a **separate service** from the endpoints above: it uses a different
base URL, a different auth header (`X-API-KEY`), and your **Database ID** — not the
`api-tenant` / `api-token` headers. It is commonly used to load lookup tables for ETL
pipelines (see [Ediphi ETL Documentation](./etl.md)).

**Base URL:** `https://data.ediphi.com/api/dataset/json`

### Authentication (Data API)

Data API requests use an API key header and a form-encoded content type:

```text
X-API-KEY: your_data_api_key
Content-Type: application/x-www-form-urlencoded
```

You also need your **Database ID**, which identifies your tenant database in the request
payload. Contact support for your Data API key and Database ID.

### Query Data (Native SQL)

```text
POST /api/dataset/json
```

Runs a native SQL query against your database and returns the matching rows as JSON.

**Request Body:** form-encoded, with a `query=` prefix wrapping a JSON object:

```text
query={"database":"your_db_id","type":"native","native":{"query":"SELECT id, name, category FROM regions WHERE deleted_at IS NULL"}}
```

Payload fields:

- `database` - Your Database ID.
- `type` - Always `native`.
- `native.query` - The SQL query to run.

**Example:**

```bash
curl -X POST "https://data.ediphi.com/api/dataset/json" \
     -H "X-API-KEY: your_data_api_key" \
     -H "Content-Type: application/x-www-form-urlencoded" \
     --data-urlencode 'query={"database":"your_db_id","type":"native","native":{"query":"SELECT id, name, category FROM regions WHERE deleted_at IS NULL"}}'
```

**Response:** A JSON array of row objects:

```json
[
  { "id": "uuid", "name": "West", "category": "Region" }
]
```

### Common Lookup Queries

Reference tables typically loaded for enrichment / ETL. Each row below is the `native.query`
sent to the endpoint above.

| Table | Columns selected | Filter |
| --- | --- | --- |
| `regions` | `id, name, category` | `deleted_at IS NULL` |
| `use_groups` | `id, name, category` | `deleted_at IS NULL` |
| `setup` | `id, name, key, value::text` | `key IN ('sort_codes:mf', 'sort_codes:uf', 'projects:industries')` |
| `sort_fields` | `id, name, key, project, standard` | `deleted_at IS NULL` |
| `sort_codes` | `id, sort_field, code, description, quantifier` | `deleted_at IS NULL` |
| `users` | `id, full_name, email, admin` | `deleted_at IS NULL` |
| `opportunities` | `id, opty_number, name, account_name, owner_name` | (none) |
| `projects` | `id, name, owner, region, opportunity, industry, client_name` | `deleted_at IS NULL` |
| `estimates` | `id, name, project, phase, stats::text, created_at, updated_at, active` | `deleted_at IS NULL` (order by `created_at DESC`) |

**Incremental loads:** filter by `updated_at` to pull only changed rows, e.g. append
`AND updated_at > 'last_run_timestamp'` to the `estimates` query.

---

## Error Responses

All errors follow this format:

```json
{
  "error": true,
  "data": "Error message describing what went wrong"
}
```

**Common HTTP Status Codes:**

- `400` - Bad Request (invalid parameters or payload)
- `403` - Forbidden (invalid API key or tenant)
- `404` - Not Found
- `500` - Internal Server Error

---

## Rate Limits

Contact your account manager for rate limit information specific to your plan.

---

## Support

For questions or issues:

1. Check that your `api-tenant` and `api-token` headers are correct
2. Ensure request payloads match the expected format
3. Contact your Ediphi support representative

---

## Notes

- All responses are in JSON format
- All dates should be in ISO 8601 format
- IDs are UUIDs
- Batch operations have limits (typically 200 items)
- Pagination applies only to `GET /vendors` and `GET /vendor_contacts`; other list endpoints
  use their own filters
- Some endpoints may require specific permissions - contact support if you receive 403
  errors for endpoints you expect to access
