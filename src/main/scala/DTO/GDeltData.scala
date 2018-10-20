package DTO

import java.sql.Timestamp

case class GDeltData (
  id: String,
  date: Timestamp,
  sourceCollectionId: Integer,
  sourceCommonNames: String,
  documentIdentifier: String,
  counts: String,
  v2Counts: String,
  themes: String,
  v2Themes: String,
  locations: String,
  v2Locations: String,
  persons: String,
  v2Persons: String,
  organizations: String,
  v2Organizations: String,
  v2Tone: String,
  dates: String,
  gCam: String,
  sharingImages: String,
  relatedImages: String,
  socialImageEmbeds: String,
  socialVideoEmbeds: String,
  quotations: String,
  allNames: String,
  amounts: String,
  translationInfo: String,
  extras: String
)