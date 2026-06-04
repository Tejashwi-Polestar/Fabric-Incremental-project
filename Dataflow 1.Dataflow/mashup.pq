[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"bronze Application Cities" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "ca7e8beb-e1ce-4d19-b434-d0a60aa1bbc4"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "175cb864-18f8-4e82-adad-cb6794a79985"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "bronze.Application.Cities", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Geography_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Geography = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "ca7e8beb-e1ce-4d19-b434-d0a60aa1bbc4"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "175cb864-18f8-4e82-adad-cb6794a79985"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "bronze.Application.Countries", ItemKind = "Table"]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 2", {"CountryID"}, #"bronze Application StateProvinces", {"CountryID"}, "bronze Application StateProvinces", JoinKind.LeftOuter),
  #"Expanded bronze Application StateProvinces" = Table.ExpandTableColumn(#"Merged queries", "bronze Application StateProvinces", {"StateProvinceID", "StateProvinceCode", "StateProvinceName", "SalesTerritory"}, {"StateProvinceID", "StateProvinceCode", "StateProvinceName", "SalesTerritory"}),
  #"Merged queries 1" = Table.NestedJoin(#"Expanded bronze Application StateProvinces", {"StateProvinceID"}, #"bronze Application Cities", {"StateProvinceID"}, "bronze Application Cities", JoinKind.LeftOuter),
  #"Expanded bronze Application Cities" = Table.ExpandTableColumn(#"Merged queries 1", "bronze Application Cities", {"CityID", "CityName"}, {"CityID", "CityName"})
in
  #"Expanded bronze Application Cities";
shared #"bronze Application StateProvinces" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "ca7e8beb-e1ce-4d19-b434-d0a60aa1bbc4"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "175cb864-18f8-4e82-adad-cb6794a79985"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "bronze.Application.StateProvinces", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared Geography_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "ca7e8beb-e1ce-4d19-b434-d0a60aa1bbc4"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "175cb864-18f8-4e82-adad-cb6794a79985"]}[Data],
  Navigation_3 = Navigation_2{[Id = "silver", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "Geography", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
