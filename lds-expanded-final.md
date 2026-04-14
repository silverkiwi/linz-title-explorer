# LDS Full Landonline Data Dictionary and Models

**Version 3.1 — July 2024 | LINZ Data Service**

*Expanded and quality-checked — tables reconstructed from PDF layout, with weblinks preserved*


## Table of Contents

- [4.1 Action](#41-action)
- [4.2 Action Type](#42-action-type)
- [4.5 Adjustment Run](#45-adjustment-run)
- [4.7 Adoption](#47-adoption)
- [4.8 Affected Parcel](#48-affected-parcel)
- [4.9 Alias](#49-alias)
- [4.10 Appellation](#410-appellation)
- [4.11 Comprised In](#411-comprised-in)
- [4.12 Coordinate](#412-coordinate)
- [4.13 Coordinate Order](#413-coordinate-order)
- [4.16 Coordinate Type](#416-coordinate-type)
- [4.17 Datum](#417-datum)
- [4.18 Ellipsoid](#418-ellipsoid)
- [4.19 Encumbrance](#419-encumbrance)
- [4.21 Encumbrancee](#421-encumbrancee)
- [4.22 Estate Share](#422-estate-share)
- [4.25 Land District](#425-land-district)
- [4.28 Line](#428-line)
- [4.29 Locality](#429-locality)
- [4.30 Maintenance](#430-maintenance)
- [4.32 Mark](#432-mark)
- [4.33 Mark Name](#433-mark-name)
- [4.36 Node](#436-node)
- [4.38 Node Works](#438-node-works)
- [4.39 Nominal Index](#439-nominal-index)
- [4.40 Observation](#440-observation)
- [4.43 Observation Set](#443-observation-set)
- [4.45 Office](#445-office)
- [4.48 Ordinate Type](#448-ordinate-type)
- [4.49 Parcel](#449-parcel)
- [4.50 Parcel Boundary](#450-parcel-boundary)
- [4.52 Parcel Label](#452-parcel-label)
- [4.53 Parcel Linestring](#453-parcel-linestring)
- [4.54 Parcel Ring](#454-parcel-ring)
- [4.55 Proprietor](#455-proprietor)
- [4.57 Reduction Run](#457-reduction-run)
- [4.59 Setup](#459-setup)
- [4.60 Site](#460-site)
- [4.61 Site Locality](#461-site-locality)
- [4.62 Statistical Area](#462-statistical-area)
- [4.64 Statute](#464-statute)
- [4.65 Statute Action](#465-statute-action)
- [4.67 Survey](#467-survey)
- [4.71 System Code](#471-system-code)
- [4.73 Title](#473-title)
- [4.74 Title Action](#474-title-action)
- [4.77 Title Estate](#477-title-estate)
- [4.78 Title Hierarchy](#478-title-hierarchy)
- [4.79 Title Instrument](#479-title-instrument)
- [4.81 Title Memorial](#481-title-memorial)
- [4.84 Transaction Type](#484-transaction-type)
- [4.85 Unit Of Measure](#485-unit-of-measure)
- [4.86 User](#486-user)
- [4.87 Vector](#487-vector)
- [4.88 Vector Point](#488-vector-point)
- [4.89 Work](#489-work)

---

## Document Information
LINZ Data Service: Full Landonline
Dataset
Data Dictionary and Data Models
July 2024 
Version 3.1


---

Contents
1 Versioning
2 Introduction
3 Data Definitions
3.1 Parcels Dataset Model
3.2 Parcel Topology Dataset Model
3.3 Survey Dataset Model
3.4 Marks, Nodes & Coordinates Dataset Model
3.5 Survey Observations Dataset Model
3.6 Adjustments Dataset Model
3.7 System / Shared Dataset Model
3.8 Title Dataset Model
3.9 Title Memorial Dataset Model
4 Data Dictionary: Introduction
4.1 Action (https://data.linz.govt.nz/table/51702)
4.2 Action Type (https://data.linz.govt.nz/table/51728)
4.3 Adjustment Coefficient (https://data.linz.govt.nz/table/51704)
4.4 Adjustment Method (https://data.linz.govt.nz/table/51705)
4.5 Adjustment Run (https://data.linz.govt.nz/table/51981)
4.6 Adjustment User Coefficient (https://data.linz.govt.nz/table/51703)
4.7 Adoption (https://data.linz.govt.nz/table/51706)
4.8 Affected Parcel (https://data.linz.govt.nz/table/51707)
4.9 Alias (https://data.linz.govt.nz/table/51982)
4.10 Appellation (https://data.linz.govt.nz/table/51590)
4.11 Comprised In (https://data.linz.govt.nz/table/51708)
4.12 Coordinate (https://data.linz.govt.nz/table/52018)
4.13 Coordinate Order (https://data.linz.govt.nz/table/51712)
4.14 Coordinate Precision (https://data.linz.govt.nz/table/51711)
4.15 Coordinate System (https://data.linz.govt.nz/table/51709)
4.16 Coordinate Type (https://data.linz.govt.nz/table/51710)
4.17 Datum (https://data.linz.govt.nz/table/51713)
4.18 Ellipsoid (https://data.linz.govt.nz/table/51715)
4.19 Encumbrance (https://data.linz.govt.nz/table/51984)
4.20 Encumbrance Share (https://data.linz.govt.nz/table/51983)
4.21 Encumbrancee (https://data.linz.govt.nz/table/51985)
4.22 Estate Share (https://data.linz.govt.nz/table/52065)
4.23 Feature Name Point (https://data.linz.govt.nz/layer/52017)
4.24 Feature Name Polygon (https://data.linz.govt.nz/layer/52016)
4.25 Land District (https://data.linz.govt.nz/layer/52070)
4.26 Legal Description (https://data.linz.govt.nz/table/51986)
4.27 Legal Description Parcel (https://data.linz.govt.nz/table/51717)
4.28 Line (https://data.linz.govt.nz/layer/51975)
4.29 Locality (https://data.linz.govt.nz/layer/51718)
4.30 Maintenance (https://data.linz.govt.nz/table/51988)
4.31 Map Grid (Deprecated) (https://data.linz.govt.nz/layer/51726)
4.32 Mark (https://data.linz.govt.nz/table/51989)
4.33 Mark Name (https://data.linz.govt.nz/table/51991)
4.34 Mark Physical State (https://data.linz.govt.nz/table/51990)
4.35 Mark Supporting Document (https://data.linz.govt.nz/table/51727)
4.36 Node (https://data.linz.govt.nz/layer/51993)
4.37 Node Proposed Order (https://data.linz.govt.nz/table/51992)
4.38 Node Works (https://data.linz.govt.nz/table/51729)
4.39 Nominal Index (https://data.linz.govt.nz/table/51994)
4.40 Observation (https://data.linz.govt.nz/table/51725)
4.41 Observation Accuracy (https://data.linz.govt.nz/table/51724)
4.42 Observation Element Type (https://data.linz.govt.nz/table/51730)
4.43 Observation Set (https://data.linz.govt.nz/table/51731)
4.44 Observation Type (https://data.linz.govt.nz/table/51732)
4.45 Office (https://data.linz.govt.nz/table/52066)
4.46 Official Coordinate System (https://data.linz.govt.nz/layer/51733)
4.47 Ordinate Adjustment (https://data.linz.govt.nz/table/89377)
4.48 Ordinate Type (https://data.linz.govt.nz/table/51735)
4.49 Parcel (https://data.linz.govt.nz/layer/51976)
4.50 Parcel Boundary (https://data.linz.govt.nz/table/51723)


---

4.51 Parcel Dimension (https://data.linz.govt.nz/table/51995)
4.52 Parcel Label (https://data.linz.govt.nz/layer/51996)
4.53 Parcel Linestring (https://data.linz.govt.nz/layer/51977)
4.54 Parcel Ring (https://data.linz.govt.nz/table/51997)
4.55 Proprietor (https://data.linz.govt.nz/table/51998)
4.56 Reduction Method (https://data.linz.govt.nz/table/51736)
4.57 Reduction Run (https://data.linz.govt.nz/table/51737)
4.58 Reference Survey (https://data.linz.govt.nz/table/51738)
4.59 Setup (https://data.linz.govt.nz/table/51742)
4.60 Site (https://data.linz.govt.nz/table/51743)
4.61 Site Locality (https://data.linz.govt.nz/table/51744)
4.62 Statistical Area (https://data.linz.govt.nz/table/52000)
4.63 Statistical Version (https://data.linz.govt.nz/table/51999)
4.64 Statute (https://data.linz.govt.nz/table/51699)
4.65 Statute Action (https://data.linz.govt.nz/table/51698)
4.66 Statutory Action Parcel (https://data.linz.govt.nz/table/51700)
4.67 Survey (https://data.linz.govt.nz/table/52001)
4.68 Survey Admin Area (https://data.linz.govt.nz/table/51746)
4.69 Survey Plan Reference (https://data.linz.govt.nz/layer/51747)
4.70 Survey Plan Image Revision (https://data.linz.govt.nz/table/52069)
4.71 System Code (https://data.linz.govt.nz/table/51648)
4.72 System Code Group (https://data.linz.govt.nz/table/51593)
4.73 Title (https://data.linz.govt.nz/table/52067)
4.74 Title Action (https://data.linz.govt.nz/table/52002)
4.75 Title Document Reference (https://data.linz.govt.nz/table/52004)
4.76 Title Encumbrance (https://data.linz.govt.nz/table/52010)
4.77 Title Estate (https://data.linz.govt.nz/table/52068)
4.78 Title Hierarchy (https://data.linz.govt.nz/table/52011)
4.79 Title Instrument (https://data.linz.govt.nz/table/52012)
4.80 Title Instrument Title (https://data.linz.govt.nz/table/52013)
4.81 Title Memorial (https://data.linz.govt.nz/table/52006)
4.82 Title Memorial Text (https://data.linz.govt.nz/table/52007)
4.83 Title Parcel Association (https://data.linz.govt.nz/table/52008)
4.84 Transaction Type (https://data.linz.govt.nz/table/52009)
4.85 Unit Of Measure (https://data.linz.govt.nz/table/51748)
4.86 User (https://data.linz.govt.nz/table/52062)
4.87 Vector (https://data.linz.govt.nz/layer/51979)
4.88 Vector Point (https://data.linz.govt.nz/layer/51980)
4.89 Work (https://data.linz.govt.nz/table/52014)


---

1 Versioning
Version
number
Amendments
Date
1.0
First draft drawn from BDE documentation for external comment. 
Notable changes: 
- Geometry type defined 
- Clarification of primary keys
September
2014
2.0
First official release
November
2014
2.1
Updates for schema and data changes, and minor corrections
May 2015
2.2
Updates to LINZ Licence For Personal Data references
September
2016
2.3
Updates for the change of source for Street Address and Roads data from Landonline to the
Address Information Management System
October
2016
2.4
Revised URLs to direct to new dataset IDs 
Added Ordinate Adjustment table and model 
Deprecated Map Grid 
Removed roads and addresses table and model
November
2017
2.5
Updated for change to Creative Commons Attribution 4.0 license
April 2018
2.6
Terminology changes from Land Transfer Act 2017
November
2018
2.7
Minor schema changes
January
2022
2.8
Minor schema changes
October
2022
2.9
Updates for to reference CCBY 4.0 and new version of LINZ License for Personal Data
May 2023
3.0
Added height_limited column to Appellation table. Changed data type of bearing_corr field in
Reference Survey table to decimal(16,12)
July 2023
3.1
Update links to LINZ Licence for Personal Data
July 2024


---

## 4.1 Action

**LDS Link:** https://data.linz.govt.nz/table/51702

*Instruments can be made up of one or more actions. Actions are used to perform the actual operations of titles transactions.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `AUDIT_ID` | INTEGER | Yes | Id used to link to the associated audit details. (Primary Key) The instrument the action belongs to. The instrument id is also included in the primary in all foreign keys from the action table. (FK) |
| `TIN_ID` | INTEGER | Yes | The instrument the action belongs to. The instrument id is also included in the primary in all foreign keys from the action table. (FK) |
| `ID` | INTEGER | Yes | Unique Identifier |
| `SEQUENCE` | INTEGER | Yes | The sequence of the action within the instrument |
| `ATT_TYPE` | VARCHAR(4) | Yes | The type of the action that describes the operations that the action can perform. (FK) |
| `SYSTEM_ACTION` | CHAR(1) | Yes | Whether the current action was created internally by CRS or created explicitly by the user. This is only set to "Yes" by new titles instruments as part of a "copy down" operation. |
| `ACT_ID_ORIG` | INTEGER | No | The id of the existing action that the current action affects. Used by the data entry screens in the Receive Registration subsystem when a new action alters or removes the data created by another "original" action. (FK) |
| `ACT_TIN_ID_ORIG` | INTEGER | No | The id of the existing instrument that the current action affects. Used by the data entry screens in the Receive Registration subsystem when a new action alters or removes the data created by another "original" instrument and action. (FK) |
| `STE_ID` | INTEGER | No | The id of the statute that is associated with this particular action. This column is used to populate variables in memorial templates for the action. (FK) |
| `MODE` | VARCHAR(4) | No | Window mode for the Modify Encumbrance screen. This column will only be used by CRR_S11 - Modify Encumbrance. See Reference Code group ACTM for valid values. |
| `FLAGS` | VARCHAR(4) | No | A general purpose column to store state during processing of the action. |
| `SOURCE` | INTEGER | Yes | Default: 1 |

## 4.2 Action Type

**LDS Link:** https://data.linz.govt.nz/table/51728

*Instruments can be made up of one or more actions. Actions are used to perform the actual operations of titles transactions. This table contains all the valid action types.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `AUDIT_ID` | INTEGER | Yes | Id used to link to the associated audit details. (Primary Key) |
| `TYPE` | VARCHAR(4) | Yes | The code for the action type. |
| `DESCRIPTION` | VARCHAR(200) | Yes | The description for the action type. Flag to indicate if the action type can be manually assigned to an instrument in Receive Registration by a user (eg, Modify Proprietor), or if it is an action that can be allocated by the system only. |
| `SYSTEM_ACTION` | CHAR(1) | Yes | Flag to indicate if the action type can be manually assigned to an instrument in Receive Registration by a user (eg, Modify Proprietor), or if it is an action that can be allocated by the system only. |
| `SOB_NAME` | VARCHAR(50) | No | The name of the window in Receive Registration that should be opened to allow the entry of structured data for this action type. (FK) |
| `EXISTING_INST` | CHAR(1) | Yes |  |

## 4.5 Adjustment Run

**LDS Link:** https://data.linz.govt.nz/table/51981

*An Adjustment is a mathematical process of generating corrections to Reduced Observations and Coordinates to generate a consistent set of adjusted Coordinates and adjusted Reduced Observations.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  |  |
| `ADM_ID` | INTEGER |  |  |
| `COS_ID` | INTEGER |  | The status of the Adjustment eg Failed, Provisional, Accepted, Authoritative. Refer Sys Code Group ADJS for valid values. |
| `STATUS` | VARCHAR(4) |  | The status of the Adjustment eg Failed, Provisional, Accepted, Authoritative. Refer Sys Code Group ADJS for valid values. |
| `USR_ID_EXEC` | VARCHAR(20) |  | the adjustment |
| `ADJUST_DATETIME` | DATETIME |  |  |
| `DESCRIPTION` | VARCHAR(100) |  | The weighted sum of squared residuals from the adjustment. With the redundancy this provides an indication of how consistent the data is and how well it fits the fixed coordinates in the adjustment. |
| `SUM_SQRD_RESIDUALS` | DECIMAL(22,12) |  | The weighted sum of squared residuals from the adjustment. With the redundancy this provides an indication of how consistent the data is and how well it fits the fixed coordinates in the adjustment. |
| `REDUNDANCY` | DECIMAL(22,12) |  | The redundancy of the adjustment is the number of independent observations - generally the total number of observations minus the number of ordinates and other parameters computed. |
| `WRK_ID` | INTEGER |  | process. |
| `AUDIT_ID` | INTEGER |  |  |

## 4.7 Adoption

**LDS Link:** https://data.linz.govt.nz/table/51706

*This entity stores the relationship between an observation element and the original observation element that the value was adopted from.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `OBN_ID_NEW` | INTEGER |  |  |
| `OBN_ID_ORIG` | INTEGER |  | as the original observation may not exist in CRS. |
| `SUR_WRK_ID_ORIG` | INTEGER |  | This is the factor that the adopted value has been altered by. For Distances the factor is multiplied by the original value. For bearings the factor is added. |
| `FACTOR_1` | DECIMAL(22,12) |  | This is the factor that the adopted value has been altered by. For Distances the factor is multiplied by the original value. For bearings the factor is added. |
| `FACTOR_2` | DECIMAL(22,12) |  | This is the factor that the adopted value has been altered by. For Distances the factor is multiplied by the original value. For bearings the factor is added. |
| `FACTOR_3` | DECIMAL(22,12) |  | This is the factor that the adopted value has been altered by. For Distances the factor is multiplied by the original value. For bearings the factor is added. |
| `AUDIT_ID` | INTEGER |  |  |

## 4.8 Affected Parcel

**LDS Link:** https://data.linz.govt.nz/table/51707

*An Affected Parcel is a Parcel which is affected by the approval of a survey dataset, including any parcels created by the approval of that survey dataset.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `SUR_WRK_ID` | INTEGER |  |  |
| `PAR_ID` | INTEGER |  | A parcel may be affected, created or extinguished by the approval of a Survey Dataset. For example, a survey can affect extinguish parcels by rendering them historical and at the same time may create new parcels (subdivision). Parcels may be affected by a survey but remain current (definition of an easement etc). Refer Sys Code Group AFPT for valid values. |
| `ACTION` | VARCHAR(4) |  | A parcel may be affected, created or extinguished by the approval of a Survey Dataset. For example, a survey can affect extinguish parcels by rendering them historical and at the same time may create new parcels (subdivision). Parcels may be affected by a survey but remain current (definition of an easement etc). Refer Sys Code Group AFPT for valid values. |
| `AUDIT_ID` | INTEGER |  |  |

## 4.9 Alias

**LDS Link:** https://data.linz.govt.nz/table/51982

*Individual registered owners may have one or more alternate names or aliases. This entity stores all of the alternate names used by an individual registered owner. Corporate owners can not have aliases.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  |  |
| `PRP_ID` | INTEGER |  |  |
| `OTHER_NAMES` | VARCHAR(100) |  | registered owner. |

## 4.10 Appellation

**LDS Link:** https://data.linz.govt.nz/table/51590

*Appellations are the textual descriptions that describe a parcel.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  |  |
| `PAR_ID` | INTEGER |  |  |
| `TYPE` | VARCHAR(4) |  | Refer Sys Code Group APPT for valid values. |
| `TITLE` | CHAR(1) |  | Valid values are "Y" or "N". |
| `SURVEY` | CHAR(1) |  | Valid values are "Y" or "N". |
| `STATUS` | VARCHAR(4) |  | Refer Sys Code Group APPS for valid values. |
| `PART_INDICATOR` | VARCHAR(4) |  | Refer Sys Code Group APPI for valid values. |
| `MAORI_NAME` | VARCHAR(100) |  | Valid for General type appellations. The sub-type of appellation used to describe the land (e.g. Deposited Plan, Survey Office Plan, Survey District, Town, etc). Valid for Maori type appellations. The plan type for a maori appellation, if applicable. Refer Sys Code Group ASAU for valid values. |
| `SUB_TYPE` | VARCHAR(4) |  | Valid for General type appellations. The sub-type of appellation used to describe the land (e.g. Deposited Plan, Survey Office Plan, Survey District, Town, etc). Valid for Maori type appellations. The plan type for a maori appellation, if applicable. Refer Sys Code Group ASAU for valid values. |
| `APPELLATION_VALUE` | VARCHAR(60) |  | Valid for General type appellations. The name or number which is associated with the appellation type (e.g., the name of the town or the number of the deposited plan). Valid for Maori type appellations. The number of the plan for a maori appellation, if applicable. |
| `PARCEL_TYPE` | VARCHAR(4) |  | Valid for General type appellations. The type of parcel used to describe the land (e.g. Lot on a deposited plan, Section on a survey office plan, etc). Refer Sys Code Group ASAP for valid values. |
| `PARCEL_VALUE` | VARCHAR(60) |  | Valid for General type appellations. The letter or number which is associated with the parcel type (e.g., '1' in Lot 1, '2' in Section 2 or 'C' in Flat C). Valid for Maori type appellations. The identifier for a Maori block. |
| `SECOND_PARCEL_TYPE` | VARCHAR(4) |  | Valid for General type appellations. The type of parcel where a secondary parcel type exists (e.g., "Section" in Lot 1 of Section 2). Refer Sys Code Group ASAP for valid values. |
| `SECOND_PRCL_VALUE` | VARCHAR(60) |  | associated with a second parcel type (e.g., "2" in Lot 1 of Section 2). |
| `BLOCK_NUMBER` | VARCHAR(15) |  | the parcel. |
| `SUB_TYPE_POSITION` | VARCHAR(4) |  | Valid for General type appellations. Indicates whether an 'appellation type' is either a suffix or a prefix to an 'appellation value'. Refer Sys Code Group AGNP for valid values. |
| `ACT_ID_CRT` | INTEGER |  | For titles appellations - the identifier of the action creating the new titles appellation. This will make the appellation current when the instrument the action belongs to is registered. |

## 4.11 Comprised In

**LDS Link:** https://data.linz.govt.nz/table/51708

*This entity contains references entered to show what the area under survey is comprised in.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  |  |
| `WRK_ID` | INTEGER |  |  |
| `TYPE` | VARCHAR(4) |  | The reference entered by the Plan Capture person. If type is title, the reference should relate to a CRS title number. Gazette notice references should be the title number if the gazette is registered, otherwise the gazette notice legality description. |
| `REFERENCE` | VARCHAR(20) |  | The reference entered by the Plan Capture person. If type is title, the reference should relate to a CRS title number. Gazette notice references should be the title number if the gazette is registered, otherwise the gazette notice legality description. |
| `LIMITED` | CHAR(1) |  | relating to a title is either fully guaranteed or "Limited as to parcels". |

## 4.12 Coordinate

**LDS Link:** https://data.linz.govt.nz/table/52018

*A set of numbers which define the position of a node relative to a coordinate system.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  |  |
| `COS_ID` | INTEGER |  |  |
| `NOD_ID` | INTEGER |  |  |
| `ORT_TYPE_1` | VARCHAR(4) |  |  |
| `ORT_TYPE_2` | VARCHAR(4) |  |  |
| `ORT_TYPE_3` | VARCHAR(4) |  |  |
| `STATUS` | VARCHAR(4) |  | Refer Sys Code Group COOS for valid values. |
| `SDC_STATUS` | CHAR(1) |  | Indicates that the coordinates have been approved as Survey-accurate Digital Cadastre (SDC) coordinates Valid values are "Y" or "N". |
| `SOURCE` | VARCHAR(4) |  | Refer Sys Code Group COOU for valid values. |
| `VALUE1` | DECIMAL(22,12) |  |  |
| `VALUE2` | DECIMAL(22,12) |  |  |
| `VALUE3` | DECIMAL(22,12) |  |  |
| `WRK_ID_CREATED` | INTEGER |  |  |
| `COR_ID` | INTEGER |  |  |
| `AUDIT_ID` | INTEGER |  |  |

## 4.13 Coordinate Order

**LDS Link:** https://data.linz.govt.nz/table/51712

*This entity contains all of the valid geodetic orders used to define the relative accuracy of coordinates.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  |  |
| `DISPLAY` | VARCHAR(4) |  |  |
| `DESCRIPTION` | VARCHAR(100) |  |  |
| `DTM_ID` | INTEGER |  | Group of order within datum for spatial window display purposes. Allows for multiple Nth order co-ordinates to easily exist in the same layer. |
| `ORDER_GROUP` | SMALLINT |  | Group of order within datum for spatial window display purposes. Allows for multiple Nth order co-ordinates to easily exist in the same layer. |
| `ERROR` | DECIMAL(12,4) |  |  |
| `AUDIT_ID` | INTEGER |  |  |

## 4.16 Coordinate Type

**LDS Link:** https://data.linz.govt.nz/table/51710

*This entity contains information about the different forms that coordinates can take within a datum.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  |  |
| `NAME` | VARCHAR(100) |  | Status of the coordinate type eg Provisional, Authoritative or Decommissioned. Refer Sys Code Group COTS for valid values |
| `STATUS` | VARCHAR(4) |  | Status of the coordinate type eg Provisional, Authoritative or Decommissioned. Refer Sys Code Group COTS for valid values |
| `ORT_TYPE_1` | VARCHAR(4) |  |  |
| `ORT_TYPE_2` | VARCHAR(4) |  |  |
| `ORT_TYPE_3` | VARCHAR(4) |  |  |
| `CATEGORY` | VARCHAR(4) |  | Refer Sys Code Group COTC for valid values |
| `DIMENSION` | VARCHAR(4) |  | Refer Sys Code Group COTD for valid values |
| `ORD_1_MIN` | DECIMAL(22,12) |  | to 180 degrees etc) |
| `ORD_1_MAX` | DECIMAL(22,12) |  | to 180 degrees etc) |
| `ORD_2_MIN` | DECIMAL(22,12) |  | to 180 degrees etc) |
| `ORD_2_MAX` | DECIMAL(22,12) |  | to 180 degrees etc) |
| `ORD_3_MIN` | DECIMAL(22,12) |  | to 180 degrees etc) |
| `ORD_3_MAX` | DECIMAL(22,12) |  | to 180 degrees etc) |
| `DATA` | VARCHAR(4) |  | Refer Sys Code Group COTA for valid values. |
| `AUDIT_ID` | INTEGER |  |  |

## 4.17 Datum

**LDS Link:** https://data.linz.govt.nz/table/51713

*A Datum is a complete system for enabling Coordinates to be assigned to Nodes. A datum is prescribed by the appropriate authority from which it derives its validity.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  |  |
| `NAME` | VARCHAR(100) |  | various names can be attached. |
| `TYPE` | VARCHAR(4) |  | Refer Sys Code Group DTMT for valid values |
| `DIMENSION` | VARCHAR(4) |  | Refer Sys Code Group DTMD for valid values |
| `REF_DATETIME` | DATETIME |  | different for a dynamic Datum) |
| `STATUS` | VARCHAR(4) |  | Refer Sys Code Group DTMS for valid values |
| `ELP_ID` | INTEGER |  | curvature of the earth |
| `REF_DATUM_CODE` | VARCHAR(4) |  | For each dimension only one reference datum should exist. All other datum’s should have a transformation to and from the reference datum. Refer Sys Code Group DTMR for valid values. |
| `CODE` | VARCHAR(10) |  |  |
| `AUDIT_ID` | INTEGER |  |  |

## 4.18 Ellipsoid

**LDS Link:** https://data.linz.govt.nz/table/51715

*Details of ellipsoid used for a datum.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  |  |
| `NAME` | VARCHAR(100) |  |  |
| `SEMI_MAJOR_AXIS` | DECIMAL(22,12) |  |  |
| `FLATTENING` | DECIMAL(22,12) |  |  |
| `AUDIT_ID` | INTEGER |  |  |

## 4.19 Encumbrance

**LDS Link:** https://data.linz.govt.nz/table/51984

*An encumbrance is an interest in the land (eg, mortgage, lease).*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  |  |
| `STATUS` | VARCHAR(4) |  | Refer Sys Code Group TSDS for valid values |
| `ACT_TIN_ID_ORIG` | INTEGER |  | (Note : this is not the abstract number) |
| `ACT_TIN_ID_CRT` | INTEGER |  |  |
| `ACT_ID_ORIG` | INTEGER |  |  |
| `ACT_ID_CRT` | INTEGER |  |  |
| `TERM` | VARCHAR(250) |  |  |

## 4.21 Encumbrancee

**LDS Link:** https://data.linz.govt.nz/table/51985

*An encumbrance on a Record of Title may be owned by one or more encumbrancees (whether an encumbrancee exists or not depends on the type of encumbrance).*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ENS_ID` | INTEGER |  |  |
| `ID` | INTEGER |  |  |
| `STATUS` | VARCHAR(4) |  | Refer Sys Code Group TSDS for valid values |
| `NAME` | VARCHAR(255) |  | The name of the encumbrancee. Names of corporate encumbrancees, and the surnames, other names and alias’s of individual encumbrancees are all held in this one attribute. |

## 4.22 Estate Share

**LDS Link:** https://data.linz.govt.nz/table/52065

*An estate may be owned in shares by registered owners. For example Marian and Anne may each own a 1/2 share in the land. This table contains one row for each share that exists in an estate.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ETT_ID` | INTEGER |  |  |
| `ID` | INTEGER |  |  |
| `STATUS` | VARCHAR(4) |  | See Reference Code Group TSDS for valid values |
| `SHARE` | VARCHAR(100) |  | The value of the share. If tenants in common exist on a title, they will be stored as separate estate shares. A value needs to be given to each of the shares. Generally the value of all the estate shares in an estate will add up to one (but this is not always the case with Maori titles). |
| `ACT_TIN_ID_CRT` | INTEGER |  | This flag is set to "Y" if this share was one of the original shares on the title. This is required for printing the historic view of the title which shows the original header data. |
| `ORIGINAL_FLAG` | CHAR(1) |  | This flag is set to "Y" if this share was one of the original shares on the title. This is required for printing the historic view of the title which shows the original header data. |
| `SYSTEM_CRT` | CHAR(1) |  | This field will indicate if an estate share has been created as as part of a copy down operation. It will be set to 'Y'es if an estate share was created by copying down another estate share simply to change the details. It will be set to 'N'o if the estate share was actually created by the user. |
| `EXECUTORSHIP` | VARCHAR(4) |  | If the share is held by an executor, this will contain the type of executorship. Examples of executorship are when a person owns a share as an administrator or as the executor of a will. See Reference Code Group ETSE for valid values. |
| `ACT_ID_CRT` | INTEGER |  | Each estate share is displayed on a separate line on the title view. This description for the estate share is automatically generated from the attributes of the estate share and the names of the registered owners and their aliases. If there are not enough details stored as structured data to generate this description, it can be manually entered and stored in this attribute. Examples of when this will be required are for certain societies and minors, where the society name or the minor's date of birth is also needed to be displayed. |
| `SHARE_MEMORIAL` | VARCHAR(17500) |  |  |

## 4.25 Land District

**LDS Link:** https://data.linz.govt.nz/layer/52070

*This entity contains all of the land districts in New Zealand*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `LOC_ID` | INTEGER |  |  |
| `OFF_CODE` | VARCHAR(4) |  |  |
| `DEFAULT` | CHAR(1) |  |  |
| `SHAPE` | GEOMETRY(POLYGON) |  | SE_ROW_ID is a unique key used internally by the ArcSDE datablade. NB the geometry stored at this location has been extracted into the SHAPE data element (above). |
| `SE_ROW_ID` | INTEGER |  | SE_ROW_ID is a unique key used internally by the ArcSDE datablade. NB the geometry stored at this location has been extracted into the SHAPE data element (above). |
| `AUDIT_ID` | INTEGER |  |  |
| `USR_TM_ID` | VARCHAR(20) |  | local unassigned queues (for internal LINZ use). |

## 4.28 Line

**LDS Link:** https://data.linz.govt.nz/layer/51975

*A Line may be a surveyed or unsurveyed boundary line. It may also be a topographical feature, or both a topographical and a boundary feature.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  |  |
| `BOUNDARY` | CHAR(1) |  | Valid values are "Y" or "N". |
| `TYPE` | VARCHAR(4) |  | Valid values are "RGHT" or "IRRE" (Refer Sys Code Group LINT) |
| `NOD_ID_END` | INTEGER |  |  |
| `NOD_ID_START` | INTEGER |  |  |
| `ARC_RADIUS` | DECIMAL(22,12) |  |  |
| `ARC_DIRECTION` | VARCHAR(4) |  | start node to end node). Refer Sys Code Group LIND for valid values. |
| `ARC_LENGTH` | DECIMAL(22,12) |  | the arc after adjustment etc. |
| `PNX_ID_CREATED` | INTEGER |  | enable topological data to be removed when a parcel is withdrawn. |
| `DCDB_FEATURE` | VARCHAR(12) |  | SE_ROW_ID is a unique key used internally by the ArcSDE datablade. NB the geometry stored at this location has been extracted into the SHAPE data element (below). |
| `SE_ROW_ID` | INTEGER |  | SE_ROW_ID is a unique key used internally by the ArcSDE datablade. NB the geometry stored at this location has been extracted into the SHAPE data element (below). |
| `AUDIT_ID` | INTEGER |  |  |
| `DESCRIPTION` | VARCHAR(2048) |  |  |
| `SHAPE` | GEOMETRY(LINE) |  |  |

## 4.29 Locality

**LDS Link:** https://data.linz.govt.nz/layer/51718

*This entity contains all localities used for searching and planning.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  |  |
| `TYPE` | VARCHAR(4) |  | Authority, Meridional Circuits Suburb, Town, etc. |
| `NAME` | VARCHAR(100) |  |  |
| `LOC_ID_PARENT` | INTEGER |  | SE_ROW_ID is a unique key used internally by the ArcSDE datablade. NB the geometry stored at this location has been extracted into the SHAPE data element (below). |
| `SE_ROW_ID` | INTEGER |  | SE_ROW_ID is a unique key used internally by the ArcSDE datablade. NB the geometry stored at this location has been extracted into the SHAPE data element (below). |
| `AUDIT_ID` | INTEGER |  |  |
| `SHAPE` | GEOMETRY(POLYGON) |  |  |

## 4.30 Maintenance

**LDS Link:** https://data.linz.govt.nz/table/51988

*This entity defines the maintenance requirements for marks, mark beacons and mark protection.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `MRK_ID` | INTEGER |  |  |
| `TYPE` | VARCHAR(4) |  | Refer Sys Code Group MNTT for valid values |
| `STATUS` | VARCHAR(4) |  | Refer Sys Code Group MNTS for valid values |
| `COMPLETE_DATE` | DATE |  |  |
| `AUDIT_ID` | INTEGER |  |  |
| `DESC` | VARCHAR(2048) |  |  |

## 4.32 Mark

**LDS Link:** https://data.linz.govt.nz/table/51989

*A Mark (MRK) is a physical monument placed for the purpose of being surveyed. A survey mark is a node which is occupied by a physical mark.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  |  |
| `NOD_ID` | INTEGER |  | The internal status of the mark e.g. Provisional, Emplaced, Surveyed, Commissioned, Decommissioned. Refer Sys Code Group MRKS for valid values |
| `STATUS` | VARCHAR(4) |  | The internal status of the mark e.g. Provisional, Emplaced, Surveyed, Commissioned, Decommissioned. Refer Sys Code Group MRKS for valid values |
| `TYPE` | VARCHAR(4) |  | Describes the physical composition of the type of mark placed as a monument as prescribed by Survey Regulations. Refer Sys Code Group MRKT for valid values |
| `CATEGORY` | VARCHAR(4) |  | Refer Sys Code Group MRKC for valid values |
| `COUNTRY` | VARCHAR(4) |  | Refer Sys Code Group CTRY for valid values |
| `BEACON_TYPE` | VARCHAR(4) |  | Refer Sys Code Group MRKE for valid values |
| `PROTECTION_TYPE` | VARCHAR(4) |  | The type of protection. Refer Sys Code Group MRKR for valid values |
| `MAINTENANCE_LEVEL` | VARCHAR(4) |  | Refer Sys Code Group MRKM for valid values. |
| `MRK_ID_DIST` | INTEGER |  |  |
| `DISTURBED` | CHAR(1) |  | Valid values are "Y" or "N". |
| `DISTURBED_DATE` | DATETIME |  |  |
| `MRK_ID_REPL` | INTEGER |  |  |
| `REPLACED` | CHAR(1) |  | Valid values are "Y" or "N". |
| `REPLACED_DATE` | DATETIME |  |  |
| `MARK_ANNOTATION` | VARCHAR(50) |  | replaced |
| `WRK_ID_CREATED` | INTEGER |  | work for re-submission). |
| `AUDIT_ID` | INTEGER |  |  |
| `DESC` | VARCHAR(2048) |  |  |

## 4.33 Mark Name

**LDS Link:** https://data.linz.govt.nz/table/51991

*This entity contains the current name, geographic name and any alternative names associated with a mark. The geographic name of a mark refers to the official feature name in the Geographic Names database which may differ from the name of a mark which is emplaced on or near that feature. A mark may have several alternative names but only one current and geographical name should exist.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `MRK_ID` | INTEGER |  |  |
| `TYPE` | VARCHAR(4) |  | Refer Sys Code Group MKNT for valid values |
| `NAME` | VARCHAR(100) |  |  |
| `AUDIT_ID` | INTEGER |  |  |

## 4.36 Node

**LDS Link:** https://data.linz.govt.nz/layer/51993

*Node can be either a physical mark or a virtual point and contains information that relates to the spatial position of that mark, its association to other marks or points, and measurements and/or calculations.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  |  |
| `COS_ID_OFFICIAL` | INTEGER |  | should exist for the node in this co-ordinate system. |
| `TYPE` | VARCHAR(4) |  | Refer Sys Code Group NODT for valid values |
| `STATUS` | VARCHAR(4) |  | Refer Sys Code Group NODS for valid values |
| `ORDER_GROUP_OFF` | INTEGER |  |  |
| `SIT_ID` | INTEGER |  |  |
| `WRK_ID_CREATED` | INTEGER |  | removed when a survey is withdrawn. |
| `ALT_ID` | INTEGER |  | record. Can be used as a warning that changes may be pending. |
| `SE_ROW_ID` | INTEGER |  | SE_ROW_ID is a unique key used internally by the ArcSDE datablade. NB the geometry stored at this location has been extracted into the SHAPE data element (below). |
| `AUDIT_ID` | INTEGER |  |  |
| `SHAPE` | GEOMETRY(POINT) |  |  |

## 4.38 Node Works

**LDS Link:** https://data.linz.govt.nz/table/51729

*An associative entity which resolves the many to many relationship between Works and Node.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `NOD_ID` | INTEGER |  |  |
| `WRK_ID` | INTEGER |  | maintenance work on the node etc. |
| `PEND_NODE_STATUS` | VARCHAR(4) |  | The status of the associated node stored as pending until the associated work is approved and then the associated node entry is updated. Refer Sys Code Group NODS for valid values. |
| `PURPOSE` | VARCHAR(4) |  | Refer Sys Code Group NOWP for valid values. |
| `ADOPTED` | CHAR(1) |  | survey. |
| `AUDIT_ID` | INTEGER |  |  |

## 4.39 Nominal Index

**LDS Link:** https://data.linz.govt.nz/table/51994

*The nominal index is used when searching for Records of Title by registered owner (formerly proprietor). The actual registered owner (proprietor) table is not used for searching. Registered owners will always be automatically copied into the nominal index, but additional entries can be manually added (or removed).*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `TTL_TITLE_NO` | VARCHAR(20) |  | If the name was automatically copied into the nominal index, this contains the id of the registered owner. This is used to make the nominal index entry historical when the registered owner is transferred off the title. |
| `PRP_ID` | INTEGER |  | If the name was automatically copied into the nominal index, this contains the id of the registered owner. This is used to make the nominal index entry historical when the registered owner is transferred off the title. |
| `ID` | INTEGER |  | (Primary Key) |
| `STATUS` | VARCHAR(4) |  | Status of the nominal index entry. This indicates if the name is currently the registered owner on the title or not. The default when searching is to search on current names only, but titles can be searched using all historical names as well. See Reference Code Group TSDS for valid values |
| `NAME_TYPE` | VARCHAR(4) |  | Reference Code Group NMIT for valid values |
| `SURNAME` | VARCHAR(100) |  | is stored here, otherwise this should be blank. |
| `OTHER_NAMES` | VARCHAR(100) |  | names are stored here, otherwise this should be blank. |

## 4.40 Observation

**LDS Link:** https://data.linz.govt.nz/table/51725

*Observation includes Reduced Observations. It may include data recorded in the field that impacts on the Observations such as meteorological observations, time of measurement, etc.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  |  |
| `OBT_TYPE` | VARCHAR(4) |  |  |
| `OBT_SUB_TYPE` | VARCHAR(4) |  | Observation Element Type table. |
| `STP_ID_LOCAL` | INTEGER |  | Refer Sys Code Group OBNT for valid values. |
| `STP_ID_REMOTE` | INTEGER |  | This is used to group observations into a set. A collection of observations which are related in some way which is useful for administration or to simplify processes. An example is a set of theodolite directions. These all have a common orientation uncertainty and are grouped as such in a network adjustment. This field is only used for geodetic observations. |
| `OBS_ID` | INTEGER |  |  |
| `COS_ID` | INTEGER |  | Identifies the reduction run that produced the reduced observation. Only the metadata relating to the reduction is stored within CRS, the actual reduction of raw observations will be performed external to CRS. |
| `RDN_ID` | INTEGER |  | Identifies the reduction run that produced the reduced observation. Only the metadata relating to the reduction is stored within CRS, the actual reduction of raw observations will be performed external to CRS. |
| `VCT_ID` | INTEGER |  | The vector that defines the graphical representation of the observation. Only one vector will exist between two nodes, observations between the same two nodes will share the same vector record. |
| `REF_DATETIME` | DATETIME |  | This is used as an offset to adjust the accuracy. Observations covariances are multiplied by the square of this number. The default value is 1. |
| `ACC_MULTIPLIER` | DECIMAL(22,12) |  | This is used as an offset to adjust the accuracy. Observations covariances are multiplied by the square of this number. The default value is 1. |
| `STATUS` | VARCHAR(4) |  | Refer Sys Code Group ROBS for valid values. |
| `GEODETIC_CLASS` | VARCHAR(4) |  | Refer Sys Code Group ROBG for valid values. |
| `CADASTRAL_CLASS` | VARCHAR(4) |  | The cadastral class of the observation. Defines the "Class" of the observation in terms of the Survey Regulations (e.g. I, II, III, IV - New Regs., and A,B,C - Old Regs). Refer Sys Code Group ROBC for valid values. |
| `SURVEYED_CLASS` | VARCHAR(4) |  | An annotation to identify the means by which the bearing was derived e.g. Measured, Calculated, Adopted. Refer Sys Code Group OBEC for valid values. For observations generated from DCDB the surveyed class will be set to Pseudo. |
| `VALUE_1` | DECIMAL(22,12) |  |  |
| `VALUE_2` | DECIMAL(22,12) |  |  |
| `VALUE_3` | DECIMAL(22,12) |  | Radius associated with an observation of type Arc. Set to Null otherwise. |
| `ARC_RADIUS` | DECIMAL(22,12) |  | Radius associated with an observation of type Arc. Set to Null otherwise. |

## 4.43 Observation Set

**LDS Link:** https://data.linz.govt.nz/table/51731

*A collection of observations which are related in some way which is useful for administration or to simplify processes.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  | Type of set (e.g. set of theodolite directions, set of GPS multi-station baselines, etc) Refer Sys Code Group OBST for valid values. |
| `TYPE` | VARCHAR(4) |  | Type of set (e.g. set of theodolite directions, set of GPS multi-station baselines, etc) Refer Sys Code Group OBST for valid values. |
| `REASON` | VARCHAR(100) |  |  |
| `AUDIT_ID` | INTEGER |  |  |

## 4.45 Office

**LDS Link:** https://data.linz.govt.nz/table/52066

*This entity contains all of the LINZ offices.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `CODE` | VARCHAR(4) |  |  |
| `NAME` | VARCHAR(50) |  |  |
| `RCS_NAME` | VARCHAR(50) |  | connect to |
| `CIS_NAME` | VARCHAR(50) |  | connect to. |
| `AUDIT_ID` | INTEGER |  | Defines the FileNet Regional Cache page_cache entry that is used to determine which alloc_table to use to generate the next barcode id for each office. |
| `ALLOC_SOURCE_TABLE` | VARCHAR(50) |  | Defines the FileNet Regional Cache page_cache entry that is used to determine which alloc_table to use to generate the next barcode id for each office. |

## 4.48 Ordinate Type

**LDS Link:** https://data.linz.govt.nz/table/51735

*This entity contains all of the ordinate types. e.g. X,Y,Z, latitude, longitude, Velocity X, Velocity Y, Velocity Z etc.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `TYPE` | VARCHAR(4) |  |  |
| `UOM_CODE` | VARCHAR(4) |  | Refer to the Unit of Measure table. |
| `DESCRIPTION` | VARCHAR(100) |  |  |
| `FORMAT_CODE` | VARCHAR(4) |  | Refer Sys Code Group ORTF for valid values. |
| `MANDATORY` | CHAR(1) |  | associated coordinate system. |
| `AUDIT_ID` | INTEGER |  |  |

## 4.49 Parcel

**LDS Link:** https://data.linz.govt.nz/layer/51976

*A Parcel is a polygon or polyhedron consisting of boundary lines (Features which are boundary features) which may be, or may be capable of being defined by survey, and includes the parcel area and appellation.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  |  |
| `LDT_LOC_ID` | INTEGER |  | Link to a diagram of the parcel. Note: This image is added manually to a parcel outside of a survey transaction and as such will be seldom used. |
| `IMG_ID` | INTEGER |  | Link to a diagram of the parcel. Note: This image is added manually to a parcel outside of a survey transaction and as such will be seldom used. |
| `FEN_ID` | INTEGER |  | Topology class of the parcel Used, sometimes in conjunction with "status," to determine the topological rules that apply to the parcels. Default value = "NONE" |
| `TOC_CODE` | VARCHAR(4) |  | Topology class of the parcel Used, sometimes in conjunction with "status," to determine the topological rules that apply to the parcels. Default value = "NONE" |
| `ALT_ID` | INTEGER |  | the record. Can be used as a warning that changes may be pending. |
| `AREA` | DECIMAL(20,4) |  | title or amend a title. |
| `NONSURVEY_DEF` | VARCHAR(255) |  | A description of a parcel, which is not currently defined by Survey. This may be a reference to a description of the parcel, an imaged document or some other definition of the parcel. |
| `APPELLATION_DATE` | DATETIME |  | The date the database is updated with the survey dataset. Allocation of new appellations may mean the appellation date differs from the other parcels on the same survey dataset. |
| `PARCEL_INTENT` | VARCHAR(4) |  | Refer Sys Code Group PARI for valid values. |
| `STATUS` | VARCHAR(4) |  | Refer Sys Code Group PARS for valid values. |
| `TOTAL_AREA` | DECIMAL(20,4) |  |  |
| `CALCULATED_AREA` | DECIMAL(20,4) |  | SE_ROW_ID is a unique key used internally by the ArcSDE datablade. NB the geometry stored at this location has been extracted into the SHAPE data element (below). |
| `SE_ROW_ID` | INTEGER |  | SE_ROW_ID is a unique key used internally by the ArcSDE datablade. NB the geometry stored at this location has been extracted into the SHAPE data element (below). |
| `AUDIT_ID` | INTEGER |  |  |
| `SHAPE` | GEOMETRY(POLYGON) |  |  |

## 4.50 Parcel Boundary

**LDS Link:** https://data.linz.govt.nz/table/51723

*This entity records the sequence of lines that define a PRI "Parcel Ring". In the sequence, a line may or may not be reversed in order for the line to connect to the next line in terms of end nodes.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `PRI_ID` | INTEGER |  |  |
| `SEQUENCE` | INTEGER |  |  |
| `LIN_ID` | INTEGER |  |  |
| `REVERSED` | CHAR(1) |  | Valid values are "Y" or "N". |
| `AUDIT_ID` | INTEGER |  |  |

## 4.52 Parcel Label

**LDS Link:** https://data.linz.govt.nz/layer/51996

*This entity stores the location of the spatial label used to annotate parcels.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  |  |
| `PAR_ID` | INTEGER |  |  |
| `SHAPE` | GEOMETRY(POINT) |  |  |
| `AUDIT_ID` | INTEGER |  | SE_ROW_ID is a unique key used internally by the ArcSDE datablade. NB the geometry stored at this location has been extracted into the SHAPE data element (above). |
| `SE_ROW_ID` | INTEGER |  | SE_ROW_ID is a unique key used internally by the ArcSDE datablade. NB the geometry stored at this location has been extracted into the SHAPE data element (above). |

## 4.53 Parcel Linestring

**LDS Link:** https://data.linz.govt.nz/layer/51977

*Non-primary parcels with a geometry type of a linestring, usually representing a centreline of an easement.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  |  |
| `LDT_LOC_ID` | INTEGER |  |  |
| `IMG_ID` | INTEGER |  | parcel outside of a survey transaction and as such will be seldom used. |
| `FEN_ID` | INTEGER |  | Topology class of the parcel Used, sometimes in conjunction with "status," to determine the topological rules that apply to the parcels. Default value = "NONE" |
| `TOC_CODE` | VARCHAR(4) |  | Topology class of the parcel Used, sometimes in conjunction with "status," to determine the topological rules that apply to the parcels. Default value = "NONE" |
| `ALT_ID` | INTEGER |  | record. Can be used as a warning that changes may be pending. |
| `AREA` | DECIMAL(20,4) |  | title or amend a title. |
| `NONSURVEY_DEF` | VARCHAR(255) |  | A description of a parcel, which is not currently defined by Survey. This may be a reference to a description of the parcel, an imaged document or some other definition of the parcel. |
| `APPELLATION_DATE` | DATETIME |  | The date the database is updated with the survey dataset. Allocation of new appellations may mean the appellation date differs from the other parcels on the same survey dataset. |
| `PARCEL_INTENT` | VARCHAR(4) |  | Refer Sys Code Group PARI for valid values. |
| `STATUS` | VARCHAR(4) |  | Refer Sys Code Group PARS for valid values. |
| `TOTAL_AREA` | DECIMAL(20,4) |  |  |
| `CALCULATED_AREA` | DECIMAL(20,4) |  | SE_ROW_ID is a unique key used internally by the ArcSDE datablade. NB the geometry stored at this location has been extracted into the SHAPE data element (below). |
| `SE_ROW_ID` | INTEGER |  | SE_ROW_ID is a unique key used internally by the ArcSDE datablade. NB the geometry stored at this location has been extracted into the SHAPE data element (below). |
| `AUDIT_ID` | INTEGER |  |  |
| `SHAPE` | GEOMETRY(LINE) |  |  |

## 4.54 Parcel Ring

**LDS Link:** https://data.linz.govt.nz/table/51997

*This entity stores one record for each ring in a polygonal (e.g. fee simple) parcel or for the line sequence of a lineal (e.g. centreline) parcel.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  |  |
| `PAR_ID` | INTEGER |  | Non-mandatory self-reference. Indicates the ring (of the same parcel) that this ring lies within. If NULL, indicates that the line sequence is an exterior ring. |
| `PRI_ID_PARENT_RING` | INTEGER |  | Non-mandatory self-reference. Indicates the ring (of the same parcel) that this ring lies within. If NULL, indicates that the line sequence is an exterior ring. |
| `IS_RING` | CHAR(1) |  | Does the line sequence form a linear ring, i.e. start and end nodes are the same? This will be true for all line sequences in a polygonal parcel, and not true for the line sequence in a lineal (e.g. centreline) parcel. Valid values are "Y" or "N". |
| `AUDIT_ID` | INTEGER |  |  |

## 4.55 Proprietor

**LDS Link:** https://data.linz.govt.nz/table/51998

*A registered owner (previously known as a proprietor) is a person or corporation holding a share in a Record of Title.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ETS_ID` | INTEGER |  | The identifier of the estate share that the registered owner owns. If there are joint tenants on a title, there will be more than one registered owner for the same estate share. |
| `ID` | INTEGER |  | Key) |
| `STATUS` | VARCHAR(4) |  | See Reference Code Group TSDS for valid values. |
| `TYPE` | VARCHAR(4) |  | Indicates whether this registered owner is an individual or corporation. See Reference Code Group PRPT for valid values. |
| `PRIME_SURNAME` | VARCHAR(100) |  | If this registered owner is an individual, the surname of the registered owner is stored here, otherwise this should be blank. |
| `PRIME_OTHER_NAMES` | VARCHAR(100) |  | If this registered owner is an individual, the given name(s) of the registered owner are stored here, otherwise this should be blank. |
| `NAME_SUFFIX` | VARCHAR(4) |  | If this registered owner is an individual, the name_suffix of the registered owner is stored here, otherwise this should be blank. See Reference Code Group NMSF for valid values. |
| `ORIGINAL_FLAG` | CHAR(1) |  | This flag is set to "Y" if this registered owner was one of the original registered owners on the title. This is required for printing the historic view of the title which shows the original header data. |

## 4.57 Reduction Run

**LDS Link:** https://data.linz.govt.nz/table/51737

*A Reduction Run is a mathematical process of reducing raw observations to generate a set of reduced observations (in the case of GPS observations, this results in the generation of baselines).*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  |  |
| `RDM_ID` | INTEGER |  |  |
| `DATETIME` | DATETIME |  |  |
| `DESCRIPTION` | VARCHAR(100) |  |  |
| `TRAJ_TYPE` | VARCHAR(4) |  | Refer Sys Code Group RDNR for valid values. |
| `USR_ID_EXEC` | VARCHAR(20) |  |  |
| `SOFTWARE_USED` | VARCHAR(30) |  |  |
| `AUDIT_ID` | INTEGER |  |  |

## 4.59 Setup

**LDS Link:** https://data.linz.govt.nz/table/51742

*The Setup entity holds information about a set-up at a Node as a result of a type of Work such as a Survey.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  |  |
| `NOD_ID` | INTEGER |  | The type of setup i.e. local or remote. Refer Sys Code Group STPT for valid values. Note that the same setup should be used for a bearing and distance combined. There should only be one bearing and one distance for any setup pair. |
| `TYPE` | VARCHAR(4) |  | The type of setup i.e. local or remote. Refer Sys Code Group STPT for valid values. Note that the same setup should be used for a bearing and distance combined. There should only be one bearing and one distance for any setup pair. |
| `VALID_FLAG` | CHAR(1) |  | Valid values are "Y" or "N". |
| `EQUIPMENT_TYPE` | VARCHAR(4) |  | Refer Sys Code Group STPE for valid values. |
| `WRK_ID` | INTEGER |  |  |
| `AUDIT_ID` | INTEGER |  |  |

## 4.60 Site

**LDS Link:** https://data.linz.govt.nz/table/51743

*A physical location which was selected, according to specifications for the placement and subsequent survey and re-survey of physical geodetic marks (nodes).*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  | Site Type (i.e. reason for grouping). Options are maintenance or proposed scheme. Refer Sys Code Group SITT for valid values. |
| `TYPE` | VARCHAR(4) |  | Site Type (i.e. reason for grouping). Options are maintenance or proposed scheme. Refer Sys Code Group SITT for valid values. |
| `OCCUPIER` | VARCHAR(100) |  |  |
| `AUDIT_ID` | INTEGER |  |  |
| `WRK_ID_CREATED` | INTEGER |  |  |
| `DESC` | VARCHAR(2048) |  |  |

## 4.61 Site Locality

**LDS Link:** https://data.linz.govt.nz/table/51744

*Associative entity that links sites and localities.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `SIT_ID` | INTEGER |  |  |
| `LOC_ID` | INTEGER |  |  |
| `AUDIT_ID` | INTEGER |  |  |

## 4.62 Statistical Area

**LDS Link:** https://data.linz.govt.nz/table/52000

*Statistical Areas are areas definable as an aggregation of meshblocks.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  |  |
| `SAV_AREA_CLASS` | VARCHAR(4) |  | Refer Sys Code Group SAVA for valid values. |
| `SAV_VERSION` | INTEGER |  | The version number of the statistical area 'layer'. This allows multiple boundaries of the same area class to be stored with different "as at" dates. |
| `NAME` | VARCHAR(100) |  |  |
| `NAME_ABREV` | VARCHAR(18) |  | A unique numeric code used to identify a specific statistical area. (Code is assigned externally to CRS but will always be unique when used in conjunction with Sav_Area_Class & Sav_Version) |
| `CODE` | VARCHAR(6) |  | A unique numeric code used to identify a specific statistical area. (Code is assigned externally to CRS but will always be unique when used in conjunction with Sav_Area_Class & Sav_Version) |
| `STATUS` | VARCHAR(4) |  |  |
| `AUDIT_ID` | INTEGER |  |  |

## 4.64 Statute

**LDS Link:** https://data.linz.govt.nz/table/51699

*A Statute is legislation enacted by Parliament and includes Acts and Regulations. The contents or provisions of a Statute (Act) are identified in terms of Parts, Sections and Schedules and the Act name. Statutory Regulations are structured in the same way.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  |  |
| `SECTION` | VARCHAR(100) |  | sub-paragraphs. |
| `NAME_AND_DATE` | VARCHAR(100) |  |  |
| `STILL_IN_FORCE` | CHAR(1) |  |  |
| `IN_FORCE_DATE` | DATE |  |  |
| `REPEAL_DATE` | DATE |  | Identifies the valid types of statutory actions that can be stored against the statute. Right Restrict, Parcel Restrict etc. See Reference Code Group STAT for valid values |
| `TYPE` | VARCHAR(4) |  | Identifies the valid types of statutory actions that can be stored against the statute. Right Restrict, Parcel Restrict etc. See Reference Code Group STAT for valid values |
| `DEFAULT` | CHAR(1) |  | The default statute for the statute type. This is used in CRS where a particular type of statute needs to be defaulted (eg, when a new title is created the statute needs to be defaulted, or the create easement screen that needs to default the easement restriction statute). |
| `AUDIT_ID` | INTEGER |  |  |

## 4.65 Statute Action

**LDS Link:** https://data.linz.govt.nz/table/51698

*A Statutory Action is the action that is authorised by a specific Part or Section of an Act.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `TYPE` | VARCHAR(4) |  | Identifies the type of statutory action eg Right Restrict, Parcel Restrict etc. Refer Sys Code Group STAT for valid values. |
| `STATUS` | VARCHAR(4) |  | Refer Sys Code Group STAS for valid values. |
| `STE_ID` | INTEGER |  |  |
| `SUR_WRK_ID_VESTING` | INTEGER |  |  |
| `GAZETTE_YEAR` | SMALLINT |  |  |
| `GAZETTE_PAGE` | INTEGER |  |  |
| `GAZETTE_TYPE` | VARCHAR(4) |  |  |
| `OTHER_LEGALITY` | VARCHAR(250) |  |  |
| `RECORDED_DATE` | DATE |  |  |
| `ID` | INTEGER |  |  |
| `AUDIT_ID` | INTEGER |  |  |
| `GAZETTE_NOTICE_ID` | INTEGER |  |  |

## 4.67 Survey

**LDS Link:** https://data.linz.govt.nz/table/52001

*Survey provides details that identify the type of survey, the purpose, and who is involved with giving authorization, preparation and taking responsibility for Work when it is lodged with Land Information NZ.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `WRK_ID` | INTEGER |  |  |
| `LDT_LOC_ID` | INTEGER |  | The series relates the survey identifier to the purpose of the survey. e.g. LT, DP, SO, ML etc. Refer Sys Code Group SURD for valid values. |
| `DATASET_SERIES` | VARCHAR(4) |  | The series relates the survey identifier to the purpose of the survey. e.g. LT, DP, SO, ML etc. Refer Sys Code Group SURD for valid values. |
| `DATASET_ID` | VARCHAR(20) |  | The number used to identify the survey. This is only unique when combined with the land district and data set series for surveys create prior to CRS. This will be unique for all new surveys created within CRS. |
| `TYPE_OF_DATASET` | VARCHAR(4) |  |  |
| `DATA_SOURCE` | VARCHAR(4) |  | Valuedflag that indicates if the survey was DCDB converted (CONV), a Work In Progress survey (WIPS), general Landonline survey (LNDL) or electronically lodged survey (ESUR). Refer Sys Code Group SURW for valid values. |
| `LODGE_ORDER` | INTEGER |  | The value of which position in the order of lodgment the survey was. This is used for staged unit plans to enable the correct suffix to be allocated. |
| `DATASET_SUFFIX` | VARCHAR(7) |  | Suffix for the dataset. This is used to provide a unique identifier for staged unit developments as they use the same dataset id for each stage. |
| `SURVEYOR_DATA_REF` | VARCHAR(100) |  | The reference attached to the dataset by the surveyor to enable the survey number to be cross referenced with the surveyors own records after lodgment. Name or number or both. |
| `SURVEY_CLASS` | VARCHAR(4) |  | Refer Sys Code Group SURC for valid values. |
| `DESCRIPTION` | VARCHAR(2048) |  | 1000" |
| `USR_ID_SOL` | VARCHAR(20) |  |  |
| `SURVEY_DATE` | DATE |  |  |
| `CERTIFIED_DATE` | DATE |  | performed the survey. |
| `REGISTERED_DATE` | DATE |  | The date the survey was deposit for LT plans The date the survey was approved for SO plans The date the survey was approved by the MLC Judge for ML plans. |
| `CHF_SUR_AMND_DATE` | DATE |  |  |
| `DLR_AMND_DATE` | DATE |  |  |

## 4.71 System Code

**LDS Link:** https://data.linz.govt.nz/table/51648

*This entity contains a maintainable list of values and parameters etc used for configuring the system.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `SCG_CODE` | VARCHAR(4) |  | Unique identifier of groups. The normal naming convention is the three letter acronym for the associated table followed by another letter e.g. MRKS - Is the list of all available Mark Statuses. |
| `CODE` | VARCHAR(4) |  |  |
| `DESC` | VARCHAR(2048) |  |  |
| `STATUS` | VARCHAR(4) |  | Refer Sys Code Group SYSS for valid values. |
| `DATE_VALUE` | DATETIME |  |  |
| `CHAR_VALUE` | VARCHAR(2048) |  |  |
| `NUM_VALUE` | DECIMAL(22,12) |  |  |
| `START_DATE` | DATE |  |  |
| `END_DATE` | DATE |  |  |
| `AUDIT_ID` | INTEGER |  |  |

## 4.73 Title

**LDS Link:** https://data.linz.govt.nz/table/52067

*A title is a record of all estates, encumbrances and easements that affect a piece of land.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `TITLE_NO` | VARCHAR(20) |  | The title number that uniquely identifies each title. Pre Landonline title numbers will usually be in the form of "nnnx/nn" (Eg, "203B/12" or "1A/1"). They will be converted with the land district prefix at the beginning to make them unique eg “OT1A/1”. New title numbers created in Landonline will be numbers only. |
| `LDT_LOC_ID` | INTEGER |  |  |
| `STATUS` | VARCHAR(4) |  | See Reference Code Group TTLS for valid values |
| `ISSUE_DATE` | DATETIME YEAR |  | Indicates the register the title is contained in (eg, the Computer Freehold Register, Computer Interest Register) See Reference Code Group TTLR for valid values. |
| `REGISTER_TYPE` | VARCHAR(4) |  | Indicates the register the title is contained in (eg, the Computer Freehold Register, Computer Interest Register) See Reference Code Group TTLR for valid values. |
| `TYPE` | VARCHAR(4) |  | Indicates the type of title. Examples of title type include Freehold, Leasehold, and Supplementary Record Sheet. See Reference Code Group TTLT for valid values. |
| `AUDIT_ID` | INTEGER |  | The id of the statute that the title is issued under. This will be the Unit Titles Act for supplementary record sheets and the Land Transfer Act for all other titles. This is displayed on the title views. |
| `STE_ID` | INTEGER |  | The id of the statute that the title is issued under. This will be the Unit Titles Act for supplementary record sheets and the Land Transfer Act for all other titles. This is displayed on the title views. |
| `GUARANTEE_STATUS` | VARCHAR(4) |  | The status of the State guarantee relating to a title. A title may be fully guaranteed or "Limited as to parcels" which means that a fully guaranteed title will not be issued for the land until a survey plan of the land has been deposited. See Reference Code Group TTLG for valid values. |
| `PROVISIONAL` | CHAR(1) |  | provisional). Used for display in the title view header. |
| `SUR_WRK_ID` | INTEGER |  | Titles of type Supplementary Record Sheet display a plan id in the title view. This contains the link to the corresponding work id of the SRS plan. |
| `MAORI_LAND` | CHAR(1) |  | ‘Y’ or ‘null’. Identifies titles which may potentially be Maori Land. It is known to contain omissions and errors and is indicative only. In many cases this is set to ‘null’ (no information) |
| `TTL_TITLE_NO_SRS` | VARCHAR(20) |  | This contains the SRS title number if the current title is a unit title. |
| `TTL_TITLE_NO_HEAD_SRS` | VARCHAR(20) |  | Titles of type Supplementary Record Sheet, where it is a subsidiary unit title development (subdivision of a principal unit), are linked back to the very first SRS in the development. This contains the head SRS title number if the current title is a SRS title for a subsidiary unit title development. |

## 4.74 Title Action

**LDS Link:** https://data.linz.govt.nz/table/52002

*Records the title that are affected by an action.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `AUDIT_ID` | INTEGER |  |  |
| `TTL_TITLE_NO` | VARCHAR(20) |  |  |
| `ACT_TIN_ID` | INTEGER |  |  |
| `ACT_ID` | INTEGER |  |  |

## 4.77 Title Estate

**LDS Link:** https://data.linz.govt.nz/table/52068

*An estate is a type of ownership of a piece of land e.g. fee simple estate, leasehold estate.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `TTL_TITLE_NO` | VARCHAR(20) |  |  |
| `TYPE` | VARCHAR(4) |  | Reference Code Group ETTT for valid values |
| `STATUS` | VARCHAR(4) |  | See Reference Code Group TSDS for valid values |
| `SHARE` | VARCHAR(100) |  | The share of the estate held by this title. This field will normally be a whole share, however, it is possible for two different titles to contain a half (or other) share of the same area of land (eg, for access lots or composite cross-lease titles). |
| `PURPOSE` | VARCHAR(255) |  | The description of the purpose that the land is held for (if applicable). Eg, a piece of land vested in the council during a subdivision may have a purpose "Local Purpose (Community Buildings) Reserve". The purpose restricts the use of the land and the title. Used for display in the title view. |
| `TIMESHARE_WEEK_NO` | VARCHAR(20) |  | the title is for. |
| `LGD_ID` | INTEGER |  |  |
| `ID` | INTEGER |  |  |
| `ACT_TIN_ID_CRT` | INTEGER |  | instrument that created the estate. |
| `ORIGINAL_FLAG` | CHAR(1) |  | This flag is set to "Y" if this is estate was one of the original estates on the title. This is required for printing the historic view of the title which shows the original header data. |
| `TIN_ID_ORIG` | INTEGER |  | The instrument number that originally created the estate. This column only requires a value for leasehold estates and computer interest registers. It is used to display on the title views of titles with a leasehold estate, or titles in the Computer Interest Register. |
| `TERM` | VARCHAR(255) |  | Description of the term of the estate. Only valid for time limited estates for example Lease Hold estates. Their term is held as a textual description only for display (ie, no automatic processing is performed based on the term). |
| `ACT_ID_CRT` | INTEGER |  | that created the estate. |

## 4.78 Title Hierarchy

**LDS Link:** https://data.linz.govt.nz/table/52011

*Lists all the prior references for the current title, which may be other prior titles or title document references.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  |  |
| `STATUS` | VARCHAR(4) |  | See Reference Code Group TSDS for valid values. |
| `TTL_TITLE_NO_PRIOR` | VARCHAR(20) |  |  |
| `TTL_TITLE_NO_FLW` | VARCHAR(20) |  |  |
| `TDR_ID` | INTEGER |  | title document. |
| `ACT_TIN_ID_CRT` | INTEGER |  |  |
| `ACT_ID_CRT` | INTEGER |  |  |

## 4.79 Title Instrument

**LDS Link:** https://data.linz.govt.nz/table/52012

*A Titles Instrument is a document relating to the transfer of, or other dealing with land.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `DLG_ID` | INTEGER |  | created through Landonline will be linked to a dealing. |
| `INST_NO` | VARCHAR(30) |  | The number of the instrument. Instrument numbers can have a number of formats, however all instruments created in Landonline will be unique and have a number in the format: . Instrument numbers prior to Landonline are not necessarily unique. |
| `PRIORITY_NO` | INTEGER |  | The order of the current instrument within the dealing. This column will always have a value for new instruments created in Landonline. It may sometimes have a value for instruments created during data conversion if it could be identified. |
| `LDT_LOC_ID` | INTEGER |  |  |
| `LODGED_DATETIME` | DATETIME YEAR TO |  | The date and time the instrument was lodged for registration. |
| `STATUS` | VARCHAR(4) |  | See Reference Code Group TINS for valid values. |
| `ID` | INTEGER |  |  |
| `TRT_GRP` | VARCHAR(4) |  |  |
| `TRT_TYPE` | VARCHAR(4) |  |  |
| `AUDIT_ID` | INTEGER |  | This field will contain the id of the title instrument that is the parent of this child instrument. An instrument is the child instrument to another if the memorial it created should be removed from the title view at the same time as the memorial created by the parent instrument (eg, a variation of mortgage instrument is a child to the mortgage instrument, as the variation memorial should be removed from the title view when the mortgage is removed). |
| `TIN_ID_PARENT` | INTEGER |  |  |

## 4.81 Title Memorial

**LDS Link:** https://data.linz.govt.nz/table/52006

*This table contains one row for each current or historical memorial for a title.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER | Yes | The unique identifier for this title memorial. (Primary Key) |
| `TTL_TITLE_NO` | VARCHAR(20) | Yes | The title the memorial appears on. (FK) |
| `MMT_CODE` | VARCHAR(10) | Yes | The memorial template code used to generate the memorial. (FK) |
| `ACT_ID_ORIG` | INTEGER | Yes | The id of the action that originally created the memorial. (FK) |
| `ACT_TIN_ID_ORIG` | INTEGER | Yes | The id of the instrument that originally created the memorial. (FK) |
| `ACT_ID_CRT` | INTEGER | Yes | The id of the action that created the memorial. (FK) |
| `ACT_TIN_ID_CRT` | INTEGER | Yes | The id of the instrument that created the memorial. (FK) |
| `STATUS` | VARCHAR(4) | Yes | The status of the title memorial. See Reference Code Group TSDS for valid values. |
| `USER_CHANGED` | CHAR(1) | Yes | This flag indicates if the user has changed the memorial text that the system generated. This is used to determine whether or not the memorial should be automatically regenerated or not. |
| `TEXT_TYPE` | VARCHAR(4) | Yes | Indicates whether or not the memorial contains text only, or contains a table. See Reference Code Group TTMT for valid values. |
| `REGISTER_ONLY_MEM` | CHAR(1) | No | Indicates whether the memorial should appear on the register copy of the title only, or if it should appear on the duplicate title as well. |
| `PREV_FURTHER_REG` | CHAR(1) | No | Indicates whether the current memorial may prevent further registration on the title, as long as it remains on thecurrent title view. |
| `CURR_HIST_FLAG` | VARCHAR(4) | Yes | This field indicates whether this memorial should be shown on the current or historic view of the title. See Reference Code Group TTMC for valid values. |
| `DEFAULT` | CHAR(1) | Yes | Flag used to determine whether the memorial was created as a default memorial. I.e. from the Instrument Detail screen. Yes/No Default: N |
| `NUMBER_OF_COLS` | INTEGER | No | If the memorial contains a table, this indicates the number of columns in the table. |
| `COL_1_SIZE` | INTEGER | No | If the memorial is a table, this indicates the size (in Powerbuilder units) of column 1. |
| `COL_2_SIZE` | INTEGER | No | If the memorial is a table, this indicates the size (in Powerbuilder units) of column 2. |
| `COL_3_SIZE` | INTEGER | No | If the memorial is a table, this indicates the size (in Powerbuilder units) of column 3. |
| `COL_4_SIZE` | INTEGER | No | If the memorial is a table, this indicates the size (in |

## 4.84 Transaction Type

**LDS Link:** https://data.linz.govt.nz/table/52009

*This entity contains the different types of transactions managed through workflow, restricted to those used in titles instruments (GRP = ‘TINT’) and survey purpose (GRP = ‘WRKT’).*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `GRP` | VARCHAR(4) |  | and Work as used in the (WRK) table for survey purpose. |
| `TYPE` | VARCHAR(4) |  |  |
| `DESCRIPTION` | VARCHAR(100) |  |  |
| `AUDIT_ID` | INTEGER |  |  |

## 4.85 Unit Of Measure

**LDS Link:** https://data.linz.govt.nz/table/51748

*This entity contains all units of measurement that are accepted into the CRS system.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `CODE` | VARCHAR(4) |  |  |
| `DESCRIPTION` | VARCHAR(100) |  | Radians, etc. |
| `AUDIT_ID` | INTEGER |  |  |

## 4.86 User

**LDS Link:** https://data.linz.govt.nz/table/52062

*Landonline user details for surveyors or survey firms who have updated data in Landonline.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | VARCHAR(20) |  |  |
| `TYPE` | VARCHAR(4) |  | Refer Sys Code Group USRT for valid values. |
| `STATUS` | VARCHAR(4) |  | Refer Sys Code Group USRS for valid values. |
| `TITLE` | VARCHAR(4) |  |  |
| `GIVEN_NAMES` | VARCHAR(30) |  |  |
| `SURNAME` | VARCHAR(30) |  |  |
| `CORPORATE_NAME` | VARCHAR(100) |  |  |
| `AUDIT_ID` | INTEGER |  |  |

## 4.87 Vector

**LDS Link:** https://data.linz.govt.nz/layer/51979

*This entity stores the details required to draw and index observations spatially. Only records with linestring or null geometries are held in this table.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `TYPE` | VARCHAR(4) |  | for valid values. |
| `NOD_ID_START` | INTEGER |  | The start node of the vector. This may be different from the local setup as the same vector will be used for all observations between the same two nodes. |
| `NOD_ID_END` | INTEGER |  | The end node of the vector. This may be different from the remote setup as the same vector will be used for all observations between the same two nodes. |
| `SOURCE` | INTEGER |  | Used to determine the source of the vector. Valid values for this field are: 0 - Pseudo observations only; 1 - At least ne cadastral observation; 2 - At least one geodetic observation; 3 - At least one cadastral and one geodetic observation |
| `SE_ROW_ID` | INTEGER |  | SE_ROW_ID is a unique key used internally by Landonline. NB the geometry stored at this location has been extracted into the SHAPE data element (below). |
| `ID` | INTEGER |  |  |
| `AUDIT_ID` | INTEGER |  |  |
| `LENGTH` | DECIMAL(22,12) |  | of the end-points. Length is 0 if the vector is a point. |
| `SHAPE` | GEOMETRY(LINE) |  |  |

## 4.88 Vector Point

**LDS Link:** https://data.linz.govt.nz/layer/51980

*This entity stores the details required to draw and index observations spatially.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `TYPE` | VARCHAR(4) |  | OBTV for valid values. |
| `NOD_ID_START` | INTEGER |  | The start node of the vector. This may be different from the local setup as the same vector will be used for all observations between the same two nodes. |
| `NOD_ID_END` | INTEGER |  | The end node of the vector. This may be different from the remote setup as the same vector will be used for all observations between the same two nodes. |
| `SOURCE` | INTEGER |  | Used to determine the source of the vector. Valid values for this field are: 0 - Pseudo observations only; 1 - At least one cadastral observation; 2 - At least one geodetic observation; 3 - At least one cadastral and one geodetic observation |
| `SE_ROW_ID` | INTEGER |  | SE_ROW_ID is a unique key used internally by Landonline. NB the geometry stored at this location has been extracted into the SHAPE data element (below). |
| `ID` | INTEGER |  |  |
| `AUDIT_ID` | INTEGER |  |  |
| `LENGTH` | DECIMAL(22,12) |  | of the end-points. Length is 0 if the vector is a point. |
| `SHAPE` | GEOMETRY(POINT) |  |  |

## 4.89 Work

**LDS Link:** https://data.linz.govt.nz/table/52014

*Provides details of the type of work being undertaken for, or supplied to Land Information NZ which has an impact on the Spatial Record.*

| Field | Type | Required | Notes |
|---|---|:---:|---|
| `ID` | INTEGER |  | The Group of Transaction e.g. Work, Instrument, Supporting Document or Request. Refer Sys Code Group TRTG for valid values. |
| `TRT_GRP` | VARCHAR(4) |  | The Group of Transaction e.g. Work, Instrument, Supporting Document or Request. Refer Sys Code Group TRTG for valid values. |
| `TRT_TYPE` | VARCHAR(4) |  | Code used to identify a transaction Sub Type. For work this will either be Mark Works, Network Scheme or the purpose of survey. See |
| `STATUS` | VARCHAR(4) |  | Refer Sys Code Groups WRKC & WRKG for valid values. |
| `CON_ID` | INTEGER |  |  |
| `PRO_ID` | INTEGER |  | This is used to integrate with workflow. |
| `USR_ID_FIRM` | VARCHAR(20) |  | The id of the external user that is considered to be responsible for the work. In the case of a geodetic works this will generally be the external approver of the work. In the case of the cadastral works, this will generally be the surveyor. |
| `USR_ID_PRINCIPAL` | VARCHAR(20) |  | The id of the external user that is considered to be responsible for the work. In the case of a geodetic works this will generally be the external approver of the work. In the case of the cadastral works, this will generally be the surveyor. |
| `CEL_ID` | INTEGER |  |  |
| `PROJECT_NAME` | VARCHAR(100) |  | The invoice may be supplied as part of the work transaction (geodetic) and before authorisation can occur, the work's invoice must be supplied for payment to be initiated. |
| `INVOICE` | VARCHAR(20) |  | The invoice may be supplied as part of the work transaction (geodetic) and before authorisation can occur, the work's invoice must be supplied for payment to be initiated. |
| `EXTERNAL_WORK_ID` | INTEGER |  | This indicates whether a user sees a transaction in their transaction listing when preparing survey's and the transaction has been authorised. Valid values are "Y" or "N". Default= "Y". |
| `VIEW_TXN` | CHAR(1) |  | This indicates whether a user sees a transaction in their transaction listing when preparing survey's and the transaction has been authorised. Valid values are "Y" or "N". Default= "Y". |
| `RESTRICTED` | CHAR(1) |  | have been filtered out by BDE) |
| `LODGED_DATE` | DATETIME |  |  |
| `AUTHORISED_DATE` | DATETIME |  |  |
| `USR_ID_AUTHORISED` | VARCHAR(20) |  |  |
| `VALIDATED_DATE` | DATE |  |  |
| `USR_ID_VALIDATED` | VARCHAR(20) |  |  |
| `COS_ID` | INTEGER |  |  |
| `DATA_LOADED` | CHAR(1) |  |  |
| `RUN_AUTO_RULES` | CHAR(1) |  | Valid values are "Y" or "N". |
| `ALT_ID` | INTEGER |  | Where present, this is a transaction id that has established a lock on the record. Can be used as a warning that changes may be pending. |
