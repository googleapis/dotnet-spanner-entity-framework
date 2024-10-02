﻿/* Copyright 2021 Google LLC
* 
* Licensed under the Apache License, Version 2.0 (the "License")
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* 
*     https://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

CREATE TABLE Singers (
  SingerId  STRING(36) NOT NULL,
  FirstName STRING(200),
  LastName  STRING(200) NOT NULL,
  BirthDate DATE,
  Picture   BYTES(MAX),
  Version   INT64 NOT NULL,
  FullName  STRING(400) NOT NULL AS (COALESCE(FirstName || ' ', '') || LastName) STORED,
) PRIMARY KEY (SingerId);

CREATE INDEX Idx_Singers_FullName ON Singers (FullName);

CREATE TABLE Albums (
  AlbumId     STRING(36) NOT NULL,
  Title       STRING(100) NOT NULL,
  ReleaseDate DATE,
  SingerId    STRING(36) NOT NULL,
  Version     INT64 NOT NULL,
  CONSTRAINT FK_Albums_Singers FOREIGN KEY (SingerId) REFERENCES Singers (SingerId),
) PRIMARY KEY (AlbumId);

CREATE TABLE Tracks (
  AlbumId         STRING(36) NOT NULL,
  TrackId         INT64 NOT NULL,
  Title           STRING(200) NOT NULL,
  Duration        NUMERIC,
  RecordedAt      TIMESTAMP DEFAULT (CURRENT_TIMESTAMP),
  LyricsLanguages ARRAY<STRING(2)>,
  Lyrics          ARRAY<STRING(MAX)>,
  Version         INT64 NOT NULL,
) PRIMARY KEY (AlbumId, TrackId), INTERLEAVE IN PARENT Albums ON DELETE CASCADE;

CREATE UNIQUE INDEX Idx_Tracks_AlbumId_Title ON Tracks (AlbumId, Title);

CREATE TABLE Venues (
  Code        STRING(10) NOT NULL,
  Name        STRING(100),
  Description JSON,
  Active      BOOL NOT NULL,
  Version     INT64 NOT NULL,
) PRIMARY KEY (Code);

CREATE TABLE Concerts (
  VenueCode STRING(10) NOT NULL,
  StartTime TIMESTAMP NOT NULL,
  SingerId  STRING(36) NOT NULL,
  Title     STRING(200),
  Version   INT64 NOT NULL,
  CONSTRAINT FK_Concerts_Venues FOREIGN KEY (VenueCode) REFERENCES Venues (Code),
  CONSTRAINT FK_Concerts_Singers FOREIGN KEY (SingerId) REFERENCES Singers (SingerId),
) PRIMARY KEY (VenueCode, StartTime, SingerId);

CREATE TABLE Performances (
  VenueCode        STRING(10) NOT NULL,
  ConcertStartTime TIMESTAMP NOT NULL,
  SingerId         STRING(36) NOT NULL,
  AlbumId          STRING(36) NOT NULL,
  TrackId          INT64 NOT NULL,
  StartTime        TIMESTAMP,
  Rating           FLOAT64,
  CreatedAt        TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  LastUpdatedAt    TIMESTAMP OPTIONS (allow_commit_timestamp=true),
  Version          INT64 NOT NULL,
  CONSTRAINT FK_Performances_Concerts FOREIGN KEY (VenueCode, ConcertStartTime, SingerId) REFERENCES Concerts (VenueCode, StartTime, SingerId),
  CONSTRAINT FK_Performances_Singers FOREIGN KEY (SingerId) REFERENCES Singers (SingerId),
  CONSTRAINT FK_Performances_Tracks FOREIGN KEY (AlbumId, TrackId) REFERENCES Tracks (AlbumId, TrackId),
) PRIMARY KEY (VenueCode, StartTime, SingerId);

CREATE SEQUENCE TicketSalesSequence OPTIONS (
    sequence_kind='bit_reversed_positive',
    start_with_counter=1,
    skip_range_min=1,
    skip_range_max=1000000
);

CREATE TABLE TicketSales (
  Id               INT64 NOT NULL DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE TicketSalesSequence)),
  CustomerName     STRING(MAX) NOT NULL,
  Seats            ARRAY<STRING(10)> NOT NULL,
  VenueCode        STRING(10) NOT NULL,
  ConcertStartTime TIMESTAMP NOT NULL,
  SingerId         STRING(36) NOT NULL,
  Version          INT64 NOT NULL,
  CONSTRAINT FK_TicketSales_Concerts FOREIGN KEY (VenueCode, ConcertStartTime, SingerId) REFERENCES Concerts (VenueCode, StartTime, SingerId),
) PRIMARY KEY (Id);
