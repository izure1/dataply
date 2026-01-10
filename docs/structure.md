# Dataply Internal Structure

> [!WARNING]
> **Dataply is currently in Alpha version.** The internal structures, page layouts, and logical identifiers described in this document are subject to change at any time without prior notice.

This document describes the physical storage structure of Pages, Rows, and Keys used in Dataply.

---

## 1. Metadata Page (Page 0)

The first page (Page 0) of every Dataply database is the **Metadata Page**. It stores global database settings and the root location of the index tree.

| Offset | Size | Field | Description |
| :--- | :--- | :--- | :--- |
| 100 | 7 | `magicString` | Identification string ('DATAPLY') |
| 108 | 4 | `pageCount` | Total number of pages in the database file |
| 112 | 4 | `pageSize` | Database page size (e.g., 8,192 bytes) |
| 116 | 6 | `rowCount` | Total number of records (rows) stored |
| 122 | 4 | `rootIndexPageId` | ID of the root page for the Primary Key index (B+Tree) |
| 126 | 4 | `rootIndexOrder` | Order (degree) of the B+Tree root node |
| 130 | 4 | `lastInsertPageId`| ID of the last data page used for insertion |
| 134 | 6 | `lastRowPk` | The highest Primary Key value used so far |
| 140 | 4 | `bitmapPageId` | ID of the first bitmap page for tracking page allocation |
| 144 | 4 | `freePageId` | ID of the first page in the free page list |

---

## 2. Page Structure

Dataply manages all data in fixed-size units called **Pages**, with a default size of **8KB (8,192 bytes)**. A page consists of a **Common Header (100 bytes)** and a **Body** where the actual data resides.

### 2-1. Common Page Header

Every page starts with a 100-byte header area.

| Offset | Size | Field | Description |
| :--- | :--- | :--- | :--- |
| 0 | 1 | `pageType` | Internal type of the page (Metadata, Bitmap, Index, Data, Overflow, etc.) |
| 1 | 4 | `pageId` | Unique ID of the current page |
| 5 | 4 | `nextPageId` | ID of the next connected page (-1 if none) |
| 9 | 4 | `insertedRowCount` | (Data Page only) Number of rows inserted |
| 13 | 4 | `remainingCapacity` | Remaining free space in the page (bytes) |
| 17 | 4 | `checksum` | CRC32 checksum for data integrity verification |
| 21-99 | 79 | Reserved | Reserved space for future extensions |

### 2-2. Data Page Layout

Data pages use a **Slotted Page** architecture for efficient record management.

- **Rows**: Stored sequentially immediately after the header (starting at Offset 100).
- **Slot Array**: Occupies 2 bytes per slot starting from the very end of the page, growing backwards. Each slot points to the start offset of a row.

```text
[Header (100B)] [Row 0] [Row 1] ... [Free Space] ... [Slot 1 (2B)] [Slot 0 (2B)]
```

---

## 3. Row Structure

A Row is the actual unit of data stored within a data page. It consists of a **Header (9 bytes)** and a **Body (Variable)**.

### 3-1. Row Header

| Offset | Size | Field | Description |
| :--- | :--- | :--- | :--- |
| 0 | 1 | `flag` | Row status flags (Bit 0: Deleted, Bit 2: Overflow) |
| 1 | 2 | `bodySize` | Pure data size of the row body (Max 65,535 bytes) |
| 3 | 6 | `pk` | 6-byte Primary Key of the row |

### 3-2. Overflow Handling

If a row's data exceeds the page's remaining capacity or the maximum page size, **Overflow Pages** are used.

1. The **Overflow Bit (Bit 2)** in the row header's `flag` is set to 1.
2. The row body stores a **4-byte Page ID** of the first overflow page instead of the actual data.
3. If the data spans multiple pages, they are connected via the `nextPageId` in the page header.

---

## 4. Index Page Layout

Used for the B+Tree index, adding index-specific metadata after the common header.

| Offset | Size | Field | Description |
| :--- | :--- | :--- | :--- |
| 100 | 4 | `indexId` | Unique Index ID |
| 104 | 4 | `parentIndexId` | ID of the parent node (page) |
| 108 | 4 | `nextIndexId` | ID of the next sibling node |
| 112 | 4 | `prevIndexId` | ID of the previous sibling node |
| 116 | 1 | `isLeaf` | Whether it is a leaf node (1: true, 0: false) |
| 117 | 4 | `keysCount` | Number of keys stored in the node |
| 121 | 4 | `valuesCount` | Number of values (child page IDs or RIDs) |
| 128 | Var | `keys & values` | 8-byte aligned array of keys and values |

---

## 5. Key and Identifier Structure

Dataply uses two types of identifiers: **Primary Key (PK)** for the user-facing identity and **Record Identifier (RID)** for internal physical addressing.

### 5-1. Primary Key (PK)

- **Size**: 6 bytes (Unsigned Integer).
- **Type**: Logical identifier.
- **Generation**: Automatically incremented upon insertion.
- **Role**: Used as the search key in the B+Tree to find the corresponding RID.

### 5-2. Record Identifier (RID)

- **Size**: 6 bytes.
- **Type**: Physical pointer.
- **Internal Composition**:
  - **Slot Index (2 bytes)**: The index in the slot array of a data page.
  - **Page ID (4 bytes)**: The ID of the page where the row is located.
- **Role**: Points directly to the location of the row in the storage file.
