sample_data ={
    "users": {
        "id": "cdac9ba6-598a-46f6-826a-3601486da569",
        "full_name": "Michael Navarro",
        "email": "michael@ediphi.com",
        "created_at": "2018-06-26 09:17:08.068+00",
        "updated_at": "2024-06-12 19:58:54.875+00",
        "provider_id": "00ue6waoa0i4zbwbd0h7",  // This comes from the client IDP if configured
        "deleted_at": "2024-10-01 14:29:19.695+00",
        "admin": true, // admins for your company / tenant
        "disabled": false, 
        "vendor": false, // for future subcontractor login
        "sysadmin": false, // for ediphi admins only
        "source": "core_form" // almost always core_form, meaning created thru login flow
    },
    "regions": {
        "id": "ec08a285-a826-46f5-90e9-c3479b18010f",
        "name": "Boston",
        "deleted_at": null,
        "created_at": "2018-03-23 16:32:03.72+00",
        "updated_at": "2018-03-23 16:32:03.72+00",
        "category": "NORTHEAST"  // these can be configured in app
    },
    "industries": { // these are configured in the app
        "id": "5a12c4d3-84a6-4913-b265-0b4a4f6590ac",
        "name": "Mixed-Use",
        "value": "MIXED_USE"
    },
    "projects": {
        "id": "80d8a417-de00-4bdc-bafa-ddd2d753fea5",
        "name": "UPC",
        "owner": "cdac9ba6-598a-46f6-826a-3601486da569",
        "created_at": "2024-07-16 20:14:25.027+00",
        "updated_at": "2024-07-31 17:50:40.432+00",
        "phase": "ACTIVE",
        "deleted_at": null,
        "industry": "MIXED_USE",  // these can be configured in app
        "region": "ec08a285-a826-46f5-90e9-c3479b18010f",
        "client_name": "Ediphi",
        "address": "100 Main Street, Boston, MA, USA",
        "task_number": "123456",
        "opportunity": null, // this is only used if our CRM PUT endpoint is used
        "test": false,
        "markup_budget": 0,
        "public": false,
        "color": "#1C1C1E", // this is a color highlight applied to the project card in app
        "archived": null,
        "lng_lat": null,
        "city": "Boston",
        "state": "MA",
        "zip": "02108"
    },
    "estimates": {
        "id": "73110315-13a3-4045-aacb-edaf6ddbba58",
        "name": "Casino and Hotel Conceptual Estimate",
        "project": "27c464be-b995-4b80-9d6b-0f7d3237004f",
        "created_at": "2019-03-22 20:10:03.689+00",
        "updated_at": "2024-09-04 12:00:18.195+00",
        "deleted_at": null,
        "updated_by": "cdac9ba6-598a-46f6-826a-3601486da569",
        "phase": "CONCEPTUAL",
        "original_estimate": null, // if duplicated from another estimate, shows lineage
        "lock": false,
        "stats": {}, // estimate meta data including use group meta data
        "active": false, // active estimate appears in the project overvuew page, only 1 per project
        "gcgr": null,
        "estimate_systems": null,
        "region": "987d70ba-d2bf-44d7-be76-3b25b1bc2500"
    },
    "products": { // products are the item in the UPC database
        "id": "cf253f48-b699-4897-ad93-f38f3a053856",
        "name": "Excavate - Foundations",
        "created_at": "2020-01-05T02:13:02.996+00:00",
        "updated_at": "2024-09-23T18:49:47.487+00:00",
        "deleted_at": null,
        "uom": "CY",
        "system": null,
        "classification": "DETAILED",
        "owner": null, // this can be a user id, which means it's in their personal UPC
        "extras": { // follows a sort_field.id: sort_code.id pattern
          "0b933ef4-ecdc-4ee0-b55f-32b75c4944ed": "45ab9a32-2032-4385-b4ba-301d87829cb8",
          "4691f1b2-4037-4324-a106-26d19af3d3aa": "42f09409-a956-4bfa-90fd-6746d418c520"
        },
        "item_code": "313526.224",
        "type": null, // used in the finish schedule [walls, floors, ceilings, base]
        "quantity_formula": {},
        "pending": null, // the user requested this product to be added to the UPC
        "created_by": "cdac9ba6-598a-46f6-826a-3601486da569",
        "children": {}, // if this is an assembly, these are the children
        "config": {
          "add_for_estimate_use_groups": false
        },
        "category": "Soil Disposal", 
        "gcgr_type": null,
        "gcgr_group": null,
        "gcgr_cost_code": null,
        "mf": {"mf1": "31 00 00", "mf2": "31 23 00", "mf3": "31 23 33"},
        "uf": {"uf1": "A", "uf2": "A10", "uf3": "A1010"},
        "notes": null
      },
    "line_items": {
        "id": "d437151a-8dbb-4939-a144-9085fd43eadd",
        "estimate": "efbba607-ec30-411a-80a9-6df5aa2d9954",
        "product": "1361b3c7-e5cf-4ecc-af71-b5f4510cc32f", // a line item is created from a product in the UPC
        "estimate_system": null,
        "quantity": 86,
        "total_cost": 0,
        "deleted_at": null,
        "created_at": "2024-08-26 15:55:55.836+00",
        "updated_at": "2024-08-26 15:58:46.4+00",
        "name": "Dishwasher",
        "total_uc": 450,
        "labor_uc": 0,
        "prod_rate": null,
        "labor_rate": null,
        "material_uc": null,
        "equip_uc": null,
        "uom": "EA",
        "quantity_formula": {},
        "extras": { // follows a sort_field.id: sort_code.id pattern
            "27bd3b5b-1082-4fd1-8e5c-01a40cb07df6": "52d500a5-d136-4ca5-9e38-a200df3f7504",
            "4691f1b2-4037-4324-a106-26d19af3d3aa": "e6889b85-8f3f-4a45-afbe-282cfc9e572c",
            "d2438a67-2359-4133-9f88-77082abc0bc9": "ca73facf-1733-4561-a587-9d6144f43adf",
            "f3f09f21-a8fd-4d63-b1ae-2854767440b0": "968e0528-18cc-4203-a43a-25410e82eb0e"
        },
        "order": "01000.001",
        "estimate_use_groups": { // follows a estimate_use_group.id: percente_allocated pattern
            "a7b19fd7-09ba-4c1f-9b3a-65b5c922a4fb": 30,
            "b4hd65ik-h678-75bd-s46g-nch5i8wmd94l": 70
        },
        "parent": null,  // if this is part of an asembly, this is the parent line item
        "type": null, // used in the finish schedule [walls, floors, ceilings, base]
        "selected_traditional_assembly": false, // true means the this is in an assembly and its quantity is used as the assembly quantity
        "notes": "Joe to review",
        "updated_by": "75970bd4-e6ff-4727-bf33-1321539ade31",
        "updated_by_at": "2024-08-26 15:58:46.398+00",
        "created_by": null,
        "alternate": null,
        "import_batch": null,
        "category": null,
        "product_original": "1361b3c7-e5cf-4ecc-af71-b5f4510cc32f", // lineage of where the line item came from
        "gcgr_line_item": null, // if this line item came from the GCGR module, this is the id of the GCGR item
        "sub_uc": null,
        "other_uc": null,
        "total_cost_text": null,
        "mf": {"mf1": "03 00 00", "mf2": "03 30 00", "mf3": "03 30 10"}, // descriptions for these codes are configured in the app
        "uf": {"uf1": "E", "uf2": "E10", "uf3": "E1060"},
        "gcgr_total_uc": null
    },
    "estimate_markups": {
        "id": "fffc6869-79a7-47c7-8969-86351849c530",
        "estimate": "d86592ad-d8f0-4771-9b16-fb7562a0ca7d",
        "description": "Fee",
        "percentage": 3,
        "value": null,
        "order": 6,
        "deleted_at": null,
        "created_at": "2019-03-22 20:06:58.348+00",
        "updated_at": "2019-03-22 20:06:58.348+00",
        "formula": null,
        "alternate": null,
        "estimate_use_groups": {}
    },
    "sort_fields":{
        "id": "4691f1b2-4037-4324-a106-26d19af3d3aa",
        "name": "Bid Package",
        "key": "Bid Package",
        "project": null, // if null means application level, else it is scoped to a project
        "deleted_at": null,
        "created_at": "2018-06-23 06:11:35.521+00",
        "updated_at": "2019-07-08 15:09:03.325+00",
        "standard": true, // shows up by defult on all projects
        "quantifier_uom": "SF",
        "project_schedule": null, // if null means application level, else it is scoped to a project
        "standard_schedule": false  // shows up by defult on all projects
      },
    "sort_codes":{ // sort fields contain sort codes
        "id": "7e120349-5a0d-4241-9ca9-6da3cab4da80",
        "code": "0330",
        "created_at": "2018-06-23T06:11:46.221+00:00",
        "deleted_at": null,
        "quantifier": null,
        "sort_field": "4691f1b2-4037-4324-a106-26d19af3d3aa",
        "updated_at": "2018-07-17T12:46:57.535+00:00",
        "description": "Concrete"
      },
    "use_groups":{ // these are application level use groups, controlled by admins 
        "id": "02147ec2-1f3a-4257-8708-2221502b0b6e",
        "name": "Apartments",
        "deleted_at": null,
        "created_at": "2018-03-27 15:29:57.244+00",
        "updated_at": "2024-07-23 23:23:30.794+00",
        "category": "RESIDENTIAL",
        "unit_cost_label": "Apartments"
      },
    "estimate_use_groups":{
        "id": "ecc51088-ad3f-465d-99ef-37123860562d",
        "label": "Core & Shell",
        "order": 1,
        "estimate": "c2abb6c3-8e42-4d79-a3b1-79914b2ba0c3",
        "use_group": "56dc6af6-80b5-42c5-b3df-c658678414fa", // connected to application level use group
        "created_at": "2024-04-15T21:07:31.643+00:00",
        "deleted_at": null,
        "updated_at": "2024-07-23T16:18:57.442+00:00",
        "exclude_area": false // if true, does not add to the project GSF
    }
}