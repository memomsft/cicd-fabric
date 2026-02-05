[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared FACT_RESERVACIONES_SNOWFLAKE = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "164dde95-5d2c-4714-8021-2eab351acea6"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "ffda55c2-ea1a-4b83-9779-8ddd90c0f347"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "FACT_RESERVACIONES_SNOWFLAKE", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared maestros_dim_huespedes_PostgreSQL = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "164dde95-5d2c-4714-8021-2eab351acea6"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "ffda55c2-ea1a-4b83-9779-8ddd90c0f347"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "maestros_dim_huespedes_PostgreSQL", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared maestros_dim_propiedades_PostgreSQL = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "164dde95-5d2c-4714-8021-2eab351acea6"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "ffda55c2-ea1a-4b83-9779-8ddd90c0f347"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "maestros_dim_propiedades_PostgreSQL", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared servicios_consumidos_csvfiles = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "164dde95-5d2c-4714-8021-2eab351acea6"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "ffda55c2-ea1a-4b83-9779-8ddd90c0f347"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "servicios_consumidos_csvfiles", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
