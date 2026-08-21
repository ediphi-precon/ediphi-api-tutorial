# Ediphi Estimate Data ETL Documentation (Platform-Agnostic)

Comprehensive guide for extracting and transforming estimate data from the Ediphi system
for loading into any data warehouse.

**Target Audience:** Data engineers implementing ETL pipelines for any platform (Snowflake,
BigQuery, Redshift, Fabric, etc.)

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Step 1: Load Lookup Tables](#step-1-load-lookup-tables)
4. [Step 2: Retrieve Estimate Data from API](#step-2-retrieve-estimate-data-from-api)
5. [Step 3: Transform and Map Data](#step-3-transform-and-map-data)
6. [Loading to Your Data Warehouse](#loading-to-your-data-warehouse)
7. [Complete Example](#complete-example)

---

## Overview

This ETL process extracts estimate data from Ediphi and transforms it into a warehouse-ready
format. The process is platform-agnostic - you provide the loading mechanism for your
specific data warehouse.

**Process Flow:**

1. **Extract lookup tables** from data.ediphi.com API (returns JSON)
2. **Extract estimate details** from Ediphi Enterprise API (returns JSON)
3. **Transform and enrich** data with lookups (Python/Pandas)
4. **Load** to your data warehouse (your implementation)

---

## Prerequisites

### Authentication

- **Tenant name**: Your Ediphi tenant identifier (e.g., sandbox)
- **Ediphi Enterprise API Key**: For `/api/external/estimates/:id` endpoint
- **Database API Key**: For data.ediphi.com lookup queries
- **Database ID**: Your database identifier in data.ediphi.com

### Required Python Libraries

```python
import requests
import pandas as pd
import json
from datetime import datetime
```

### API Configuration

```python
CONFIG = {
    # Ediphi Enterprise API (for estimates)
    'estimate_api': {
        'url': 'https://api.ediphi.com',
        'tenant': 'your_tenant',
        'api_key': 'your_enterprise_api_key'
    },

    # Data.Ediphi API (for lookup tables)
    'data_api': {
        'url': 'https://data.ediphi.com/api/dataset/json',
        'api_key': 'your_data_api_key',
        'database_id': 'your_db_id'
    }
}
```

---

## Step 1: Load Lookup Tables

Load all reference data needed for mapping. Data is returned as JSON from data.ediphi.com API.

### Helper Function: Query Data API

```python
def query_data_api(sql_query, config):
    """
    Query data.ediphi.com API and return results as list of dicts

    Args:
        sql_query: SQL query string
        config: Dict with url, api_key, database_id

    Returns:
        List of dictionaries
    """
    payload_dict = {
        "database": config['database_id'],
        "type": "native",
        "native": {"query": sql_query}
    }

    # API requires form-encoded payload with 'query=' prefix
    payload = f"query={json.dumps(payload_dict)}"

    headers = {
        'X-API-KEY': config['api_key'],
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.post(config['url'], headers=headers, data=payload)
    response.raise_for_status()
    return response.json()
```

### Load All Lookup Tables

```python
def load_all_lookups(data_api_config):
    """
    Load all lookup tables and return as pandas DataFrames

    Returns:
        Dict of DataFrames keyed by table name
    """
    lookups = {}

    # 1. Regions
    print("Loading regions...")
    regions_data = query_data_api(
        "SELECT id, name, category FROM regions WHERE deleted_at IS NULL",
        data_api_config
    )
    lookups['regions'] = pd.DataFrame(regions_data)
    print(f"  ✓ Loaded {len(lookups['regions'])} regions")

    # 2. Use Groups
    print("Loading use_groups...")
    use_groups_data = query_data_api(
        "SELECT id, name, category FROM use_groups WHERE deleted_at IS NULL",
        data_api_config
    )
    lookups['use_groups'] = pd.DataFrame(use_groups_data)
    print(f"  ✓ Loaded {len(lookups['use_groups'])} use groups")

    # 3. Setup Table (MF/UF codes and industries)
    print("Loading setup table...")
    setup_data = query_data_api(
        """
        SELECT id, name, key, value::text as value
        FROM setup
        WHERE key IN ('sort_codes:mf', 'sort_codes:uf', 'projects:industries')
        """,
        data_api_config
    )
    setup_df = pd.DataFrame(setup_data)

    # Flatten hierarchical MF/UF codes
    mf_uf_codes = flatten_mf_uf_codes(setup_df)
    lookups['mf_uf_codes'] = pd.DataFrame(mf_uf_codes)
    print(f"  ✓ Loaded {len(lookups['mf_uf_codes'])} MF/UF codes")

    # Flatten industries
    industries = flatten_industries(setup_df)
    lookups['industries'] = pd.DataFrame(industries)
    print(f"  ✓ Loaded {len(lookups['industries'])} industries")

    # 4. Sort Fields (for line item extras)
    print("Loading sort_fields...")
    sort_fields_data = query_data_api(
        "SELECT id, name, key, project, standard FROM sort_fields WHERE deleted_at IS NULL",
        data_api_config
    )
    lookups['sort_fields'] = pd.DataFrame(sort_fields_data)
    print(f"  ✓ Loaded {len(lookups['sort_fields'])} sort fields")

    # 5. Sort Codes (for line item extras)
    print("Loading sort_codes...")
    sort_codes_data = query_data_api(
        """
        SELECT id, sort_field, code, description, quantifier
        FROM sort_codes
        WHERE deleted_at IS NULL
        """,
        data_api_config
    )
    lookups['sort_codes'] = pd.DataFrame(sort_codes_data)
    print(f"  ✓ Loaded {len(lookups['sort_codes'])} sort codes")

    # 6. Users
    print("Loading users...")
    users_data = query_data_api(
        "SELECT id, full_name, email, admin FROM users WHERE deleted_at IS NULL",
        data_api_config
    )
    lookups['users'] = pd.DataFrame(users_data)
    print(f"  ✓ Loaded {len(lookups['users'])} users")

    # 7. Opportunities
    print("Loading opportunities...")
    opportunities_data = query_data_api(
        """
        SELECT id, opty_number, name, account_name, owner_name
        FROM opportunities
        """,
        data_api_config
    )
    lookups['opportunities'] = pd.DataFrame(opportunities_data)
    print(f"  ✓ Loaded {len(lookups['opportunities'])} opportunities")

    # 8. Projects
    print("Loading projects...")
    projects_data = query_data_api(
        """
        SELECT id, name, owner, region, opportunity, industry, client_name
        FROM projects
        WHERE deleted_at IS NULL
        """,
        data_api_config
    )
    lookups['projects'] = pd.DataFrame(projects_data)
    print(f"  ✓ Loaded {len(lookups['projects'])} projects")

    # 9. Estimates (list to process)
    print("Loading estimates...")
    estimates_data = query_data_api(
        """
        SELECT id, name, project, phase, stats::text as stats,
               created_at, updated_at, active
        FROM estimates
        WHERE deleted_at IS NULL
        ORDER BY created_at DESC
        """,
        data_api_config
    )
    lookups['estimates'] = pd.DataFrame(estimates_data)
    print(f"  ✓ Loaded {len(lookups['estimates'])} estimates")

    return lookups


def flatten_mf_uf_codes(setup_df):
    """Flatten hierarchical MF/UF setup data to flat list"""
    flat_codes = []

    def traverse_tree(nodes, setup_key):
        for node in nodes:
            flat_codes.append({
                'setup_key': setup_key,
                'code': node['code'],
                'description': node['description']
            })
            if node.get('children'):
                traverse_tree(node['children'], setup_key)

    for _, record in setup_df.iterrows():
        if record['key'] in ['sort_codes:mf', 'sort_codes:uf']:
            value_data = json.loads(record['value'])
            traverse_tree(value_data, record['key'])

    return flat_codes


def flatten_industries(setup_df):
    """Flatten industries from setup data"""
    industries = []

    for _, record in setup_df.iterrows():
        if record['key'] == 'projects:industries':
            value_data = json.loads(record['value'])
            for item in value_data:
                industries.append({
                    'industry_key': item['key'],
                    'industry_name': item['name'],
                    'sort_order': item.get('sortOrder', 0)
                })

    return industries
```

### Summary of Lookup Tables

After Step 1, you have these pandas DataFrames:

| DataFrame Key | Purpose | Row Count (typical) |
| --- | --- | --- |
| `regions` | Project locations | 5-20 |
| `use_groups` | Usage categories | 5-50 |
| `mf_uf_codes` | MF/UF sort codes | 1000-5000 |
| `industries` | Project industries | 5-20 |
| `sort_fields` | Custom sort field definitions | 5-50 |
| `sort_codes` | Custom sort code values | 500-2000 |
| `users` | System users | 10-200 |
| `opportunities` | CRM opportunities | 1000-30000 |
| `projects` | Projects | 50-500 |
| `estimates` | Estimates to process | 50-1000 |

---

## Step 2: Retrieve Estimate Data from API

For each targeted estimate, call the Ediphi Enterprise API to get detailed estimate data.

### Fetch Estimate from API

```python
def fetch_estimate(estimate_id, api_config):
    """
    Fetch detailed estimate from Ediphi Enterprise API

    Args:
        estimate_id: UUID of estimate
        api_config: Dict with url, tenant, api_key

    Returns:
        Dict with estimate, line_items, estimate_use_groups, estimate_markups, etc.
    """
    response = requests.get(
        f"{api_config['url']}/api/external/estimates/{estimate_id}",
        headers={
            'api-tenant': api_config['tenant'],
            'api-token': api_config['api_key']
        }
    )

    if response.status_code == 200:
        return response.json()['sanitizedEstimate']
    else:
        raise Exception(f"API Error: {response.status_code} - {response.text}")
```

### Understanding API Response

The API returns a dict with these keys:

- **estimate**: Main estimate record with `stats` field (pre-calculated totals)
- **line_items**: Array of cost line items with `mf`, `uf`, `extras`, `estimate_use_groups`
- **estimate_use_groups**: Use group associations for the estimate
- **estimate_markups**: Markup configurations
- **estimate_systems**: System associations
- **area_matrix_items**: Floor/level rows for the estimate (geometry + structural
  classification)
- **area_matrix_item_estimate_use_groups**: Join table allocating floors to use groups
  (holds area allocation)
- **geometry_worksheets**: Dimension/quantity worksheets (width/height/length/diameter/quantity
  as `ValueAndFormula` JSONB), optionally tied to a `line_item`
- Plus many other related entities (rooms, fixtures, etc.)

### Important: estimate.stats Field

The `stats` field contains pre-calculated summary data:

```json
{
  "grandTotal": 2178254.00,
  "totalUnits": 5,
  "markupsTotal": 651419.99,
  "totalLineItems": 50,
  "grossArea": 10000,
  "costOfTradesSubtotal": 1526835.36,
  "useGroups": [
    {
        "id": "7f62b434-47df-4da1-857f-bbfedd900e7d",
        "gsf": 10000,
        "nsf": 0,
        "label": null,
        "order": 1,
        "totals": {
            "area": 0,
            "beds_total": 0,
            "unit_count": 5,
            "baths_total": 0,
            "unit_area_total": 0,
            "half_baths_total": 0,
            "terminal_hvacs_total": 0
        },
        "estimate": "db1e4c9d-87b9-4cad-a494-9b28f1e92c65",
        "use_group": "Hotel",
        "unit_count": 0,
        "exclude_area": false,
        "use_group_id": "264f197a-40ec-44d6-899a-41cefbaff4f9",
        "unit_cost_label": null,
        "costOfTradesSubtotal": 4712035,
        "totals_cost_denominators": {}
    }
  ]
}
```

**Always use `stats` for estimate-level totals** - don't recalculate from line items.

---

## Step 3: Transform and Map Data

Transform the raw API data using the lookup tables.

### 3.1: Enrich Estimate Metadata

```python
def enrich_estimate(estimate, lookups):
    """
    Enrich estimate with project, region, industry, etc.

    Args:
        estimate: Estimate dict from API
        lookups: Dict of lookup DataFrames

    Returns:
        Enriched estimate dict
    """
    enriched = estimate.copy()

    # Get project
    project = lookups['projects'][lookups['projects']['id'] == estimate['project']]
    if not project.empty:
        project = project.iloc[0].to_dict()
        enriched['project_name'] = project.get('name')
        enriched['client_name'] = project.get('client_name')
        enriched['industry_key'] = project.get('industry')

        # Get region
        region = lookups['regions'][lookups['regions']['id'] == project.get('region')]
        if not region.empty:
            enriched['region_name'] = region.iloc[0]['name']

        # Get industry
        industry = lookups['industries'][lookups['industries']['industry_key'] == project.get('industry')]
        if not industry.empty:
            enriched['industry_name'] = industry.iloc[0]['industry_name']

        # Get opportunity
        opp = lookups['opportunities'][lookups['opportunities']['id'] == project.get('opportunity')]
        if not opp.empty:
            enriched['opportunity_number'] = opp.iloc[0]['opty_number']

    # Get user
    user = lookups['users'][lookups['users']['id'] == estimate.get('updated_by')]
    if not user.empty:
        enriched['updated_by_name'] = user.iloc[0]['full_name']

    # Parse stats field
    if estimate.get('stats'):
        stats = json.loads(estimate['stats']) if isinstance(estimate['stats'], str) else estimate['stats']
        enriched['stats_parsed'] = stats
        enriched['grand_total'] = stats.get('grandTotal')
        enriched['subtotal'] = stats.get('subtotal')
        enriched['total_labor'] = stats.get('totalLabor')
        enriched['total_material'] = stats.get('totalMaterial')
        enriched['total_equipment'] = stats.get('totalEquipment')

    return enriched
```

### 3.2: Enrich Line Items - MF/UF Codes

```python
def enrich_line_items_mf_uf(line_items, lookups):
    """
    Enrich line items with MF/UF descriptions from setup

    Args:
        line_items: List of line item dicts
        lookups: Dict of lookup DataFrames

    Returns:
        List of enriched line item dicts
    """
    # Create lookup dicts for performance
    mf_codes_df = lookups['mf_uf_codes']
    mf_lookup = mf_codes_df[mf_codes_df['setup_key'] == 'sort_codes:mf'].set_index('code')['description'].to_dict()
    uf_lookup = mf_codes_df[mf_codes_df['setup_key'] == 'sort_codes:uf'].set_index('code')['description'].to_dict()

    enriched_items = []

    for item in line_items:
        enriched_item = item.copy()

        # Enrich MF codes
        if item.get('mf'):
            mf_data = item['mf'] if isinstance(item['mf'], dict) else json.loads(item['mf'])
            mf_descriptions = {}

            for field_key, code_value in mf_data.items():
                description = mf_lookup.get(code_value)
                if description:
                    mf_descriptions[f'{field_key}_code'] = code_value
                    mf_descriptions[f'{field_key}_description'] = description

            enriched_item['mf_enriched'] = mf_descriptions

        # Enrich UF codes
        if item.get('uf'):
            uf_data = item['uf'] if isinstance(item['uf'], dict) else json.loads(item['uf'])
            uf_descriptions = {}

            for field_key, code_value in uf_data.items():
                description = uf_lookup.get(code_value)
                if description:
                    uf_descriptions[f'{field_key}_code'] = code_value
                    uf_descriptions[f'{field_key}_description'] = description

            enriched_item['uf_enriched'] = uf_descriptions

        enriched_items.append(enriched_item)

    return enriched_items
```

### 3.3: Enrich Line Items - Extras

```python
def enrich_line_items_extras(line_items, lookups):
    """
    Enrich line items extras field with sort field/code descriptions

    Line item extras structure:
    - Key: sort_field_id (UUID)
    - Value: sort_code_id (UUID)

    Args:
        line_items: List of line item dicts
        lookups: Dict of lookup DataFrames

    Returns:
        List of enriched line item dicts
    """
    # Create lookup dicts
    sort_fields_lookup = lookups['sort_fields'].set_index('id').to_dict('index')
    sort_codes_lookup = lookups['sort_codes'].set_index('id').to_dict('index')

    enriched_items = []

    for item in line_items:
        enriched_item = item.copy()

        if item.get('extras'):
            extras_data = item['extras'] if isinstance(item['extras'], dict) else json.loads(item['extras'])
            extras_enriched = {}

            for sort_field_id, sort_code_id in extras_data.items():
                sort_field = sort_fields_lookup.get(sort_field_id, {})
                sort_code = sort_codes_lookup.get(sort_code_id, {})

                if sort_field and sort_code:
                    field_key = sort_field.get('key', sort_field_id)
                    extras_enriched[field_key] = {
                        'sort_field_id': sort_field_id,
                        'sort_field_name': sort_field.get('name'),
                        'sort_code_id': sort_code_id,
                        'sort_code_code': sort_code.get('code'),
                        'sort_code_description': sort_code.get('description')
                    }

            enriched_item['extras_enriched'] = extras_enriched

        enriched_items.append(enriched_item)

    return enriched_items
```

### 3.4: Enrich Line Items - Use Group Allocations

```python
def enrich_line_items_use_groups(line_items, estimate_use_groups, lookups):
    """
    Enrich line item use group allocations

    Line item estimate_use_groups structure:
    - Key: estimate_use_group.id (UUID)
    - Value: allocation percentage (integer, e.g., 50 = 50%)

    Args:
        line_items: List of line item dicts
        estimate_use_groups: List of estimate_use_group dicts from API
        lookups: Dict of lookup DataFrames

    Returns:
        List of enriched line item dicts
    """
    # Create lookup from estimate_use_groups
    eug_lookup = {eug['id']: eug for eug in estimate_use_groups}

    # Create use_groups lookup
    use_groups_lookup = lookups['use_groups'].set_index('id').to_dict('index')

    enriched_items = []

    for item in line_items:
        enriched_item = item.copy()

        if item.get('estimate_use_groups'):
            ug_data = item['estimate_use_groups'] if isinstance(item['estimate_use_groups'], dict) else json.loads(item['estimate_use_groups'])
            ug_enriched = {}

            for estimate_ug_id, allocation_pct in ug_data.items():
                estimate_ug = eug_lookup.get(estimate_ug_id, {})
                use_group_id = estimate_ug.get('use_group')
                use_group = use_groups_lookup.get(use_group_id, {})

                ug_enriched[estimate_ug_id] = {
                    'allocation_percentage': allocation_pct,
                    'use_group_id': use_group_id,
                    'use_group_name': use_group.get('name'),
                    'use_group_category': use_group.get('category')
                }

            enriched_item['use_groups_enriched'] = ug_enriched

        enriched_items.append(enriched_item)

    return enriched_items
```

### 3.5: Enrich Estimate Markups

```python
def enrich_estimate_markups(estimate_markups):
    """
    Enrich estimate markups with calculated totals

    Args:
        estimate_markups: List of markup dicts from API

    Returns:
        List of enriched markup dicts
    """
    enriched_markups = []

    for markup in estimate_markups:
        enriched_markup = markup.copy()

        # Calculate markup total
        if markup.get('formula'):
            formula = markup['formula']
            if formula.get('overridden'):
                total = formula.get('total', 0)
            else:
                total = formula.get('total', 0) * (markup.get('percentage', 0) / 100)

            enriched_markup['calculated_total'] = total
        else:
            enriched_markup['calculated_total'] = None

        enriched_markups.append(enriched_markup)

    return enriched_markups
```

### 3.6: Enrich Area Matrix Items

```python
def enrich_area_matrix_items(area_matrix_items, lookups):
    """
    Enrich area_matrix_items (floors/levels) with floor_group info.

    Each row represents a horizontal level within an estimate and carries
    geometry (facade_perimeter, facade_floor_to_floor, facade_party_wall,
    gross_structure) plus structural classification (floor_type).

    Table columns:
      - id, estimate (FK), floor_number, floor_label, floor_type
        ('BELOW_GRADE' | 'ABOVE_GRADE' | 'FEATURE')
      - facade_perimeter, facade_floor_to_floor, facade_party_wall,
        gross_structure
      - floor_group (FK → floor_groups, nullable)

    Args:
        area_matrix_items: List of area_matrix_item dicts from API
        lookups: Dict of lookup DataFrames (expects optional 'floor_groups')

    Returns:
        List of enriched area_matrix_item dicts
    """
    floor_groups_lookup = {}
    if 'floor_groups' in lookups and not lookups['floor_groups'].empty:
        floor_groups_lookup = lookups['floor_groups'].set_index('id').to_dict('index')

    enriched_items = []

    for item in area_matrix_items:
        enriched_item = item.copy()

        # Resolve floor_group FK → name
        fg_id = item.get('floor_group')
        if fg_id:
            fg = floor_groups_lookup.get(fg_id, {})
            enriched_item['floor_group_name'] = fg.get('name')

        # Pre-compute a signed floor number for easy sorting
        # (BELOW_GRADE floors are typically negative in the UI)
        if item.get('floor_type') == 'BELOW_GRADE':
            enriched_item['signed_floor_number'] = -abs(item.get('floor_number', 0))
        else:
            enriched_item['signed_floor_number'] = item.get('floor_number', 0)

        enriched_items.append(enriched_item)

    return enriched_items
```

### 3.7: Enrich Area Matrix Item Use Group Allocations

```python
def enrich_area_matrix_item_use_groups(ami_use_groups, estimate_use_groups, lookups):
    """
    Enrich area_matrix_item_estimate_use_groups with floor + use_group context.

    This is the join table that allocates floors (area_matrix_items) to use
    groups (estimate_use_groups). The `area` column holds the allocated area
    value (NOT a percentage — it's the actual square footage assigned to that
    floor/use-group pair).

    Table columns:
      - id
      - area_matrix_item (FK → area_matrix_items.id)
      - estimate_use_group (FK → estimate_use_groups.id)
      - area (real) — allocated area
      - active_floor (bool)
      - facade_systems (jsonb, free-form) — system config, shape is not enforced
      - structure_systems (jsonb, free-form) — system config, shape is not enforced

    Args:
        ami_use_groups: List of area_matrix_item_estimate_use_group dicts from API
        estimate_use_groups: List of estimate_use_group dicts from API
        lookups: Dict of lookup DataFrames (expects 'use_groups')

    Returns:
        List of enriched allocation dicts
    """
    eug_lookup = {eug['id']: eug for eug in estimate_use_groups}
    use_groups_lookup = lookups['use_groups'].set_index('id').to_dict('index')

    enriched_rows = []

    for row in ami_use_groups:
        enriched_row = row.copy()

        # Resolve estimate_use_group → underlying use_group name/category
        estimate_ug = eug_lookup.get(row.get('estimate_use_group'), {})
        use_group_id = estimate_ug.get('use_group')
        use_group = use_groups_lookup.get(use_group_id, {})

        enriched_row['use_group_id'] = use_group_id
        enriched_row['use_group_name'] = use_group.get('name')
        enriched_row['use_group_category'] = use_group.get('category')

        # Parse JSONB system configs (shape not enforced — keep raw for downstream)
        for jsonb_field in ('facade_systems', 'structure_systems'):
            value = row.get(jsonb_field)
            if isinstance(value, str):
                try:
                    enriched_row[jsonb_field] = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    enriched_row[jsonb_field] = {}
            elif value is None:
                enriched_row[jsonb_field] = {}

        enriched_rows.append(enriched_row)

    return enriched_rows
```

### 3.8: Enrich Geometry Worksheets

```python
def enrich_geometry_worksheets(geometry_worksheets, line_items):
    """
    Enrich geometry_worksheets with line_item description and parsed dimensions.

    A geometry worksheet stores dimension inputs (width, height, length,
    diameter, quantity) that feed quantity formulas on an associated line_item.
    Each dimension is JSONB conforming to the `ValueAndFormula` shape:

        { "value": <number>, "quantity_formula": { "parts": [...] } }

    Table columns:
      - id, estimate (FK), line_item (FK, nullable), description
      - width, height, length, diameter (jsonb, ValueAndFormula, nullable)
      - quantity (jsonb, ValueAndFormula, default { "value": 1 })
      - geometry_worksheet_group (FK, nullable)

    Args:
        geometry_worksheets: List of geometry_worksheet dicts from API
        line_items: List of line_item dicts from API (for description lookup)

    Returns:
        List of enriched geometry_worksheet dicts
    """
    line_items_lookup = {li['id']: li for li in line_items}

    dimension_fields = ('width', 'height', 'length', 'diameter', 'quantity')

    enriched_worksheets = []

    for ws in geometry_worksheets:
        enriched_ws = ws.copy()

        # Attach line_item description for reporting
        li = line_items_lookup.get(ws.get('line_item'), {})
        enriched_ws['line_item_description'] = li.get('description')

        # Parse dimension JSONB columns → flat *_value fields for easy querying
        for field in dimension_fields:
            raw = ws.get(field)
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    raw = None

            if isinstance(raw, dict):
                enriched_ws[f'{field}_value'] = raw.get('value')
                enriched_ws[f'{field}_has_formula'] = bool(raw.get('quantity_formula'))
            else:
                enriched_ws[f'{field}_value'] = None
                enriched_ws[f'{field}_has_formula'] = False

        enriched_worksheets.append(enriched_ws)

    return enriched_worksheets
```

### 3.9: Complete Transformation Function

```python
def transform_estimate(estimate_data, lookups):
    """
    Complete transformation of estimate data

    Args:
        estimate_data: Raw estimate data from API
        lookups: Dict of lookup DataFrames

    Returns:
        Dict with enriched estimate, line_items, markups, area_matrix_items,
        area_matrix_item_estimate_use_groups, and geometry_worksheets
    """
    # Extract components
    estimate = estimate_data['estimate']
    line_items = estimate_data.get('line_items', [])
    estimate_use_groups = estimate_data.get('estimate_use_groups', [])
    estimate_markups = estimate_data.get('estimate_markups', [])
    area_matrix_items = estimate_data.get('area_matrix_items', [])
    ami_use_groups = estimate_data.get('area_matrix_item_estimate_use_groups', [])
    geometry_worksheets = estimate_data.get('geometry_worksheets', [])

    # Enrich estimate
    enriched_estimate = enrich_estimate(estimate, lookups)

    # Enrich line items (chain transformations)
    enriched_line_items = line_items.copy()
    enriched_line_items = enrich_line_items_mf_uf(enriched_line_items, lookups)
    enriched_line_items = enrich_line_items_extras(enriched_line_items, lookups)
    enriched_line_items = enrich_line_items_use_groups(enriched_line_items, estimate_use_groups, lookups)

    # Enrich markups
    enriched_markups = enrich_estimate_markups(estimate_markups)

    # Enrich area matrix + its use-group allocations + geometry worksheets
    enriched_ami = enrich_area_matrix_items(area_matrix_items, lookups)
    enriched_ami_ugs = enrich_area_matrix_item_use_groups(ami_use_groups, estimate_use_groups, lookups)
    enriched_geometry = enrich_geometry_worksheets(geometry_worksheets, line_items)

    # Convert to pandas DataFrames for easier loading
    return {
        'estimate': pd.DataFrame([enriched_estimate]),
        'line_items': pd.DataFrame(enriched_line_items),
        'estimate_use_groups': pd.DataFrame(estimate_use_groups),
        'estimate_markups': pd.DataFrame(enriched_markups),
        'area_matrix_items': pd.DataFrame(enriched_ami),
        'area_matrix_item_estimate_use_groups': pd.DataFrame(enriched_ami_ugs),
        'geometry_worksheets': pd.DataFrame(enriched_geometry),
    }
```

---

## Loading to Your Data Warehouse

The transformed data is returned as pandas DataFrames. Implement your warehouse-specific
loading logic.

### Generic Loading Pattern

```python
def load_to_warehouse(transformed_data, warehouse_loader):
    """
    Load transformed data to warehouse

    Args:
        transformed_data: Dict of DataFrames from transform_estimate()
        warehouse_loader: Your warehouse-specific loader object

    You implement warehouse_loader with methods like:
        - load_estimates(df)
        - load_line_items(df)
        - load_markups(df)
    """
    # Load estimates
    warehouse_loader.load_estimates(
        transformed_data['estimate'],
        table_name='estimates_fact',
        if_exists='append'
    )

    # Load line items
    warehouse_loader.load_line_items(
        transformed_data['line_items'],
        table_name='line_items_fact',
        if_exists='append'
    )

    # Load markups
    warehouse_loader.load_markups(
        transformed_data['estimate_markups'],
        table_name='estimate_markups_fact',
        if_exists='append'
    )
```

---

## Complete Example

Full end-to-end ETL script:

```python
#!/usr/bin/env python3
"""
Ediphi Estimate ETL - Platform Agnostic

Extract estimates from Ediphi, transform with lookups,
and load to any data warehouse.
"""

import requests
import pandas as pd
import json
from datetime import datetime

# ====================================================================
# Configuration
# ====================================================================

CONFIG = {
    'estimate_api': {
        'url': 'https://api.ediphi.com',
        'tenant': 'your_tenant',
        'api_key': 'your_enterprise_api_key'
    },
    'data_api': {
        'url': 'https://data.ediphi.com/api/dataset/json',
        'api_key': 'your_data_api_key',
        'database_id': 'your_db_id'
    }
}

# ====================================================================
# Helper Functions (from sections above)
# ====================================================================

def query_data_api(sql_query, config):
    """Query data.ediphi.com API"""
    payload_dict = {
        "database": config['database_id'],
        "type": "native",
        "native": {"query": sql_query}
    }

    payload = f"query={json.dumps(payload_dict)}"
    headers = {
        'X-API-KEY': config['api_key'],
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.post(config['url'], headers=headers, data=payload)
    response.raise_for_status()
    return response.json()


def fetch_estimate(estimate_id, api_config):
    """Fetch estimate from Enterprise API"""
    response = requests.get(
        f"{api_config['url']}/api/external/estimates/{estimate_id}",
        headers={
            'api-tenant': api_config['tenant'],
            'api-token': api_config['api_key']
        }
    )

    if response.status_code == 200:
        return response.json()['sanitizedEstimate']
    else:
        raise Exception(f"API Error: {response.status_code}")

# ... (Include all transformation functions from sections above)

# ====================================================================
# Main ETL Process
# ====================================================================

def run_etl(config, warehouse_loader):
    """
    Main ETL process

    Args:
        config: Configuration dict
        warehouse_loader: Your warehouse-specific loader
    """
    print("=" * 60)
    print("EDIPHI ESTIMATE ETL")
    print("=" * 60)

    # Step 1: Load lookup tables
    print("\nStep 1: Loading lookup tables...")
    lookups = load_all_lookups(config['data_api'])

    # Step 2: Process each estimate
    print("\nStep 2: Processing estimates...")
    estimates_to_process = lookups['estimates']  # Ideally only process where estimate.updated_at > last etl run date

    success_count = 0
    error_count = 0

    for idx, estimate_row in estimates_to_process.iterrows():
        estimate_id = estimate_row['id']
        estimate_name = estimate_row['name']

        try:
            print(f"\nProcessing [{idx+1}/{len(estimates_to_process)}]: {estimate_name}")

            # Fetch from API
            estimate_data = fetch_estimate(estimate_id, config['estimate_api'])

            # Transform
            transformed = transform_estimate(estimate_data, lookups)

            # Load to warehouse
            load_to_warehouse(transformed, warehouse_loader)

            success_count += 1
            print(f"  ✓ Success")

        except Exception as e:
            error_count += 1
            print(f"  ✗ Error: {str(e)}")

    # Summary
    print("\n" + "=" * 60)
    print(f"ETL Complete: {success_count} succeeded, {error_count} failed")
    print("=" * 60)


# ====================================================================
# Usage
# ====================================================================

if __name__ == '__main__':
    # Initialize your warehouse loader
    warehouse_loader = YourWarehouseLoader()

    # Run ETL
    run_etl(CONFIG, warehouse_loader)
```

---

## Best Practices

### 1. Error Handling

```python
failed_estimates = []

for estimate_row in estimates_to_process.iterrows():
    try:
        # Process estimate
        pass
    except Exception as e:
        failed_estimates.append({
            'estimate_id': estimate_row['id'],
            'error': str(e),
            'timestamp': datetime.now()
        })

# Save failures for retry
if failed_estimates:
    pd.DataFrame(failed_estimates).to_csv('failed_estimates.csv', index=False)
```

### 2. Incremental Loading

```python
# Get last successful run timestamp
last_run = get_last_run_timestamp()

# Filter estimates
estimates_query = f"""
SELECT id, name, project, stats::text as stats
FROM estimates
WHERE deleted_at IS NULL
  AND updated_at > '{last_run}'
ORDER BY updated_at
"""
```

### 3. Batch Processing

```python
# Process in batches
batch_size = 100

for i in range(0, len(estimates_to_process), batch_size):
    batch = estimates_to_process[i:i+batch_size]

    for estimate_row in batch.iterrows():
        # Process estimate
        pass

    # Commit batch to warehouse
    warehouse_loader.commit()
```

---

## Summary

This platform-agnostic ETL documentation provides:

1. **Extract**: Query lookup tables via data.ediphi.com API (returns JSON/DataFrames)
2. **Extract**: Fetch estimates via Ediphi Enterprise API (returns JSON)
3. **Transform**: Enrich with lookups using pure Python/Pandas
4. **Load**: You implement warehouse-specific loading

**Key Points:**

- All data extraction returns pandas DataFrames
- All transformation logic is pure Python
- You implement the final loading step for your warehouse

---

**Document Version:** 2.0
**Last Updated:** 2026-04-13
**For:** Platform-agnostic data warehouse loading
