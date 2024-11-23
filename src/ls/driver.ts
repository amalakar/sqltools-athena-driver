import AbstractDriver from "@sqltools/base-driver";
import {
  IConnectionDriver,
  MConnectionExplorer,
  NSDatabase,
  Arg0,
  ContextValue,
  IQueryOptions,
} from "@sqltools/types";
import queries from "./queries";
import { v4 as generateId } from "uuid";
import { Athena } from "@aws-sdk/client-athena";
import {
  GetQueryResultsInput,
  GetQueryResultsOutput,
} from "@aws-sdk/client-athena";
import { AwsCredentialIdentity } from "@aws-sdk/types";
import { fromIni } from "@aws-sdk/credential-provider-ini";
import { GetQueryExecutionCommandOutput } from "@aws-sdk/client-athena";

export default class AthenaDriver
  extends AbstractDriver<Athena, any>
  implements IConnectionDriver
{
  queries = queries;

  /**
   * If you driver depends on node packages, list it below on `deps` prop.
   * It will be installed automatically on first use of your driver.
   */
  public readonly deps: (typeof AbstractDriver.prototype)["deps"] = [
    {
      type: AbstractDriver.CONSTANTS.DEPENDENCY_PACKAGE,
      name: "lodash",
      // version: 'x.x.x',
    },
  ];

  private schema: { 
    [catalog: string]: { 
      [schema: string]: { 
        [table: string]: { 
          columnName: string, 
          dataType: string, 
          isNullable: boolean 
        }[] 
      } 
    } 
  } = {};
  private schemaLoaded: Promise<void> | null = null;

  private staticCompletions: { [w: string]: NSDatabase.IStaticCompletion } | null = null;
  private staticCompletionLoaded: Promise<void> | null = null;
  private functionDetails: { [w: string]: NSDatabase.IStaticCompletion } = {};

  private async loadInformationSchema() {
    try {
      this.log.info('About to load schema using information schema query');
      const query = `
        SELECT 
          table_catalog,
          table_schema,
          table_name,
          column_name,
          data_type,
          comment,
          is_nullable
        FROM information_schema.columns
        WHERE table_schema NOT IN ('information_schema')
      `;
      
      const result = await this.singleQuery(query, {});
      let columnCount = 0;

      let schema = {}
      result.results.forEach(row => {
        if (!schema[row['table_catalog']]) schema[row['table_catalog']] = {};
        if (!schema[row['table_catalog']][row['table_schema']]) schema[row['table_catalog']][row['table_schema']] = {};
        if (!schema[row['table_catalog']][row['table_schema']][row['table_name']]) schema[row['table_catalog']][row['table_schema']][row['table_name']] = [];
        
        schema[row['table_catalog']][row['table_schema']][row['table_name']].push({
          columnName: row['column_name'],
          dataType: row['data_type'],
          isNullable: row['is_nullable'] === 'YES'
        });
        columnCount++;

      });
      this.schema = schema;
      this.log.info({ 
        msg: 'Schema loaded using information schema',
        catalogs: Object.keys(this.schema),
        totalColumns: columnCount
      });
    } catch (error) {
      this.log.error('Failed to load schema information:', {error: error});
    }
  }

  private async loadStaticCompletions() {

    try {
      this.log.info('About to load athena functions');
      const result = await this.singleQuery('SHOW FUNCTIONS', {});
      result.results.forEach((row: { 
        functionName: string;
        argumentTypes: string;
        returnType: string;
        description: string;
        functionType: string;
        isDeterministic: boolean;
    }) => {
      let functionName = row['Function']
      let argumentTypes = row['Argument Types']
      let returnType = row['Return Type']
      let description = row['Description']
      let functionType = row['Function Type']
      let isDeterministic = row['Deterministic']

      if (!functionName) return;
      this.functionDetails[functionName] = {
        label: functionName,
        detail: `${functionName}(${argumentTypes}) → ${returnType}`,
        documentation: { 
          kind: 'markdown',
          value: description + `\n\n**Type:** ${functionType}\n**Deterministic:** ${isDeterministic}`
        }
        };
      });
      this.log.info({
        msg: 'Athena function details loaded',
        totalFunctions: Object.keys(this.functionDetails).length,
        functionDetails: this.functionDetails
      });
    } catch (error) {
      this.log.error('Failed to load athena functions:', {error: error});
    }

    // Add SQL keywords (these aren't included in SHOW FUNCTIONS)
    const keywords: { [w: string]: NSDatabase.IStaticCompletion } = {
      'SELECT': { label: 'SELECT', detail: 'SELECT keyword', documentation: { kind: 'markdown', value: 'Start a SELECT query' } },
      'FROM': { label: 'FROM', detail: 'FROM keyword', documentation: { kind: 'markdown', value: 'Specify the table to query from' } },
      'WHERE': { label: 'WHERE', detail: 'WHERE clause', documentation: { kind: 'markdown', value: 'Filter results' } },
      'GROUP BY': { label: 'GROUP BY', detail: 'GROUP BY clause', documentation: { kind: 'markdown', value: 'Group results' } },
      'ORDER BY': { label: 'ORDER BY', detail: 'ORDER BY clause', documentation: { kind: 'markdown', value: 'Sort results' } },
      'HAVING': { label: 'HAVING', detail: 'HAVING clause', documentation: { kind: 'markdown', value: 'Filter grouped results' } },
      'LIMIT': { label: 'LIMIT', detail: 'LIMIT clause', documentation: { kind: 'markdown', value: 'Limit number of results' } },
      'WITH': { label: 'WITH', detail: 'WITH clause', documentation: { kind: 'markdown', value: 'Define named subqueries' } },
      'UNNEST': { label: 'UNNEST', detail: 'UNNEST operator', documentation: { kind: 'markdown', value: 'Expands an array into a relation' } },
      'CROSS JOIN UNNEST': { label: 'CROSS JOIN UNNEST', detail: 'CROSS JOIN UNNEST operator', documentation: { kind: 'markdown', value: 'Join with expanded array' } },  
    };

    this.staticCompletions = { ...this.functionDetails, ...keywords };
  }

  public async open() {
    if (this.connection) {
      return this.connection;
    }

    let credentials: AwsCredentialIdentity;
    if (this.credentials.connectionMethod !== "Profile") {
      credentials = {
        accessKeyId: this.credentials.accessKeyId,
        secretAccessKey: this.credentials.secretAccessKey,
        sessionToken: this.credentials.sessionToken,
      };
    } else {
      try {
        credentials = await fromIni({ profile: this.credentials.profile })();
      } catch (error) {
        this.log.error(
          "Failed to use credentials for profile",
          this.credentials.profile,
          error
        );
        throw error;
      }
    }
    
    this.connection = Promise.resolve(
      new Athena({
        credentials: credentials,
        region: this.credentials.region || "us-east-1",
      })
    );

    // Start both schema and completions loading asynchronously
    this.schemaLoaded = this.loadInformationSchema().finally(() => {
      this.schemaLoaded = null;
    });

    this.staticCompletionLoaded = this.loadStaticCompletions().finally(() => {
      this.staticCompletionLoaded = null;
    });

    return this.connection;
  }

  /**
   * Format bytes to a human readable string
   */
  private formatBytes = (bytes: number, decimals: number = 2) => {
    if (!+bytes) return "0 Bytes";

    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = [
      "Bytes",
      "KiB",
      "MiB",
      "GiB",
      "TiB",
      "PiB",
      "EiB",
      "ZiB",
      "YiB",
    ];

    const i = Math.floor(Math.log(bytes) / Math.log(k));

    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`;
  };

  public async close() {


  }

  private sleep = (time: number) =>
    new Promise((resolve) => setTimeout(() => resolve(true), time));

  private async rawQuery(query: string) {
    const db = await this.open();

    const queryExecution = await db.startQueryExecution({
      QueryString: query,
      WorkGroup: this.credentials.workgroup,
      ResultConfiguration: {
        OutputLocation: this.credentials.outputLocation,
      },
    });

    const endStatus = new Set(["FAILED", "SUCCEEDED", "CANCELLED"]);

    let queryCheckExecution: GetQueryExecutionCommandOutput;

    do {
      queryCheckExecution = await db.getQueryExecution({
        QueryExecutionId: queryExecution.QueryExecutionId,
      });

      this.log.info(
        `Query ${queryExecution.QueryExecutionId} ` +
          `is ${queryCheckExecution.QueryExecution.Status.State} ` +
          `${queryCheckExecution.QueryExecution.Statistics?.TotalExecutionTimeInMillis} ms elapsed. ` +
          `${this.formatBytes(
            queryCheckExecution.QueryExecution.Statistics?.DataScannedInBytes
          )} scanned`
      );

      await this.sleep(200);
    } while (!endStatus.has(queryCheckExecution.QueryExecution.Status.State));

    if (queryCheckExecution.QueryExecution.Status.State === "FAILED") {
      throw new Error(
        queryCheckExecution.QueryExecution.Status.StateChangeReason
      );
    }

    return queryCheckExecution;
  }

  
  private async getQueryResults(queryExecutionId: string) {
    const results: GetQueryResultsOutput[] = [];
    let result: GetQueryResultsOutput;
    let nextToken: string | null = null;
    let db = await this.open();

    do {
      const payload: GetQueryResultsInput = {
        QueryExecutionId: queryExecutionId,
      };
      if (nextToken) {
        payload.NextToken = nextToken;
        await this.sleep(200);
      }
      result = await db.getQueryResults(payload);
      nextToken = result?.NextToken;
      results.push(result);
    } while (nextToken);

    return results;
  }

  public query: (typeof AbstractDriver)["prototype"]["query"] = async (
    queries,
    opt = {}
  ) => {
    const queryExecution = await this.rawQuery(queries.toString());
    const results = await this.getQueryResults(
      queryExecution.QueryExecution?.QueryExecutionId || ""
    );
    const columns = results[0].ResultSet.ResultSetMetadata.ColumnInfo.map(
      (info) => info.Name
    );
    const resultSet = [];
    results.forEach((result, i) => {
      const rows = result.ResultSet.Rows;
      if (i === 0) {
        rows.shift();
      }
      rows.forEach(({ Data }) => {
        resultSet.push(
          Object.assign(
            {},
            ...Data.map((column, i) => ({ [columns[i]]: column.VarCharValue }))
          )
        );
      });
    });

    const response: NSDatabase.IResult[] = [
      {
        cols: columns,
        connId: this.getId(),
        messages: [
          {
            date: new Date(),
            message:
              `Query "${queryExecution.QueryExecution?.QueryExecutionId}" ` +
              `ok with ${resultSet.length} results. ` +
              `${this.formatBytes(
                queryExecution?.QueryExecution?.Statistics
                  ?.DataScannedInBytes || 0
              )} scanned`,
          },
        ],
        results: resultSet,
        query: queries.toString(),
        requestId: opt.requestId,
        resultId: generateId(),
      },
    ];

    return response;
  };

  public singleQuery: (typeof AbstractDriver)["prototype"]["singleQuery"] = async (
    query: string | String,
    opt: IQueryOptions = {}
  ): Promise<NSDatabase.IResult> => {
    const results = await this.query(query.toString(), opt);
    return results[0];
  };

  /** if you need a different way to test your connection, you can set it here.
   * Otherwise by default we open and close the connection only
   */
  public async testConnection() {
    await this.open();
    await this.query("SELECT 1", {});
  }

  /**
   * This method is a helper to generate the connection explorer tree.
   * it gets the child items based on current item
   */
  public async getChildrenForItem({
    item,
    parent,
  }: Arg0<IConnectionDriver["getChildrenForItem"]>) {
    const db = await this.connection;

    switch (item.type) {
      case ContextValue.CONNECTION:
      case ContextValue.CONNECTED_CONNECTION:
        return <MConnectionExplorer.IChildItem[]>[
          {
            label: "Catalogs",
            type: ContextValue.RESOURCE_GROUP,
            iconId: "folder",
            childType: ContextValue.SCHEMA,
          },
        ];
      case ContextValue.SCHEMA:
        return <MConnectionExplorer.IChildItem[]>[
          {
            label: "Databases",
            type: ContextValue.RESOURCE_GROUP,
            iconId: "folder",
            childType: ContextValue.DATABASE,
          },
        ];
      case ContextValue.DATABASE:
        return <MConnectionExplorer.IChildItem[]>[
          {
            label: "Tables",
            type: ContextValue.RESOURCE_GROUP,
            iconId: "folder",
            childType: ContextValue.TABLE,
          },
          {
            label: "Views",
            type: ContextValue.RESOURCE_GROUP,
            iconId: "folder",
            childType: ContextValue.VIEW,
          },
        ];
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        const tableMetadata = await db.getTableMetadata({
          CatalogName: item.schema,
          DatabaseName: item.database,
          TableName: item.label,
        });

        return [
          ...(tableMetadata.TableMetadata.Columns || []),
          ...(tableMetadata.TableMetadata.PartitionKeys || []),
        ].map((column) => ({
          label: column.Name,
          type: ContextValue.COLUMN,
          dataType: column.Type,
          schema: item.schema,
          database: item.database,
          childType: ContextValue.NO_CHILD,
          isNullable: true,
          iconName: "column",
          table: parent,
        }));
      case ContextValue.RESOURCE_GROUP:
        return this.getChildrenForGroup({ item, parent });
    }

    return [];
  }

  /**
   * This method is a helper to generate the connection explorer tree.
   * It gets the child based on child types
   */
  private async getChildrenForGroup({
    parent,
    item,
  }: Arg0<IConnectionDriver["getChildrenForItem"]>) {
    if (this.schemaLoaded) {
      await this.schemaLoaded;
    }

    switch (item.childType) {
      case ContextValue.SCHEMA:
        // Get catalogs (top level)
        const catalogNames = Object.keys(this.schema);
        return catalogNames.map((catalogName) => ({
          database: "",
          label: catalogName,
          type: item.childType,
          schema: catalogName,
          childType: ContextValue.DATABASE,
        }));

      case ContextValue.DATABASE:
        // Get schemas within the catalog
        const schemas = this.schema[parent.schema] || {};
        return Object.keys(schemas).map(schemaName => ({
          database: schemaName,
          label: schemaName,
          type: item.childType,
          schema: parent.schema, // catalog
          childType: ContextValue.TABLE,
        }));

      case ContextValue.TABLE:
      case ContextValue.VIEW:
        // Get tables within the catalog and schema
        const tables = this.schema[parent.schema]?.[parent.database] || {};
        return Object.keys(tables).map(tableName => ({
          database: parent.database, // schema
          label: tableName,
          type: item.childType,
          schema: parent.schema, // catalog
          childType: ContextValue.COLUMN,
        }));
    }
    return [];
  }

  /**
   * This method is a helper for intellisense and quick picks.
   */
  public async searchItems(
    itemType: ContextValue,
    search: string,
    _extraParams: any = {}
  ): Promise<NSDatabase.SearchableItem[]> {
    // Wait for schema if it's still loading
    if (this.schemaLoaded) {
      await this.schemaLoaded;
    }

    switch (itemType) {
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        const matchingTables: NSDatabase.SearchableItem[] = [];
        
        for (const [catalogName, schemas] of Object.entries(this.schema)) {
          for (const [schemaName, tables] of Object.entries(schemas)) {
            for (const tableName of Object.keys(tables)) {
              if (!search || tableName.toLowerCase().includes(search.toLowerCase())) {
                matchingTables.push({
                  database: schemaName,
                  label: tableName,
                  type: itemType,
                  schema: catalogName,
                  childType: ContextValue.COLUMN,
                });
              }
            }
          }
        }

        return matchingTables.slice(0, 10);

      case ContextValue.COLUMN:
        const matchingColumns: NSDatabase.SearchableItem[] = [];
        
        for (const [catalogName, schemas] of Object.entries(this.schema)) {
          for (const [schemaName, tables] of Object.entries(schemas)) {
            for (const [tableName, columns] of Object.entries(tables)) {
              Object.values(columns).forEach(column => {
                if (!search || column.columnName.toLowerCase().includes(search.toLowerCase())) {
                  matchingColumns.push({
                    database: schemaName,
                    label: column.columnName,
                    type: ContextValue.COLUMN,
                    dataType: column.dataType,
                    schema: catalogName,
                    childType: ContextValue.NO_CHILD,
                    isNullable: column.isNullable,
                    iconName: "column",
                    table: tableName,
                  });
                }
              });
            }
          }
        }
        case ContextValue.FUNCTION:
          // Wait for completions if they're still loading
          if (this.staticCompletionLoaded) {
            await this.staticCompletionLoaded;
          }

          const matchingFunctions = Object.entries(this.functionDetails)
            .filter(([name]) => 
              !search || name.toLowerCase().includes(search.toLowerCase()))
            .map(([name, completion]) => ({
              label: name,
              type: ContextValue.FUNCTION,
              schema: '',
              database: '',
              childType: ContextValue.NO_CHILD,
              detail: completion.detail,
            }))
            .slice(0, 10);

          return matchingFunctions;
    }
    return [];
  }

  /**
   * Use Athena's SHOW FUNCTIONS to get completions for SQL functions
   */
  public async getStaticCompletions(): Promise<{ [w: string]: NSDatabase.IStaticCompletion }> {
    // Wait for completions if they're still loading
    if (this.staticCompletionLoaded) {
      await this.staticCompletionLoaded;
    }
    
    return this.staticCompletions || {};
  }

  public async describeTable(
    table: NSDatabase.ITable,
    opt?: IQueryOptions
  ): Promise<NSDatabase.IResult[]> {
    const db = await this.open();
    
    const tableMetadata = await db.getTableMetadata({
      CatalogName: table.schema,
      DatabaseName: table.database,
      TableName: table.label,
    });

    const columns = [
      ...(tableMetadata.TableMetadata.Columns || []),
      ...(tableMetadata.TableMetadata.PartitionKeys || [])
    ].map(col => ({
      column_name: col.Name,
      data_type: col.Type,
      is_nullable: 'YES',
      column_key: col.Name in (tableMetadata.TableMetadata.PartitionKeys || []) ? 'PARTITION KEY' : '',
    }));

    return [{
      resultId: generateId(),
      connId: this.getId(),
      cols: ['column_name', 'data_type', 'is_nullable', 'column_key'],
      messages: [{ message: `Described table ${table.label}`, date: new Date() }],
      query: `DESCRIBE \`${table.database}\`.\`${table.label}\``,
      results: columns,
      requestId: opt?.requestId,
    }];
  }
  
  public async showRecords(
    table: NSDatabase.ITable,
    opt: IQueryOptions & { limit: number; page?: number }
  ): Promise<NSDatabase.IResult[]> {
    const offset = opt.page ? (opt.page - 1) * opt.limit : 0;
    const query = `SELECT *
      FROM "${table.database}"."${table.label}"
      LIMIT ${opt.limit}
      ${offset > 0 ? `OFFSET ${offset}` : ''}`;

    const queryExecution = await this.rawQuery(query);
    const results = await this.getQueryResults(
      queryExecution.QueryExecution?.QueryExecutionId || ''
    );

    // Get column names from the first result's metadata
    const columns = results[0].ResultSet.ResultSetMetadata.ColumnInfo.map(
      (info) => info.Name
    );

    // Transform the results into the expected format
    const resultSet = [];
    results.forEach((result, i) => {
      const rows = result.ResultSet.Rows;
      if (i === 0) rows.shift(); // Skip header row in first result
      rows.forEach(({ Data }) => {
        resultSet.push(
          Object.assign(
            {},
            ...Data.map((column, i) => ({ [columns[i]]: column.VarCharValue }))
          )
        );
      });
    });

    return [{
      resultId: generateId(),
      connId: this.getId(),
      cols: columns,
      messages: [{
        date: new Date(),
        message: `Retrieved ${resultSet.length} records from ${table.label}. ${this.formatBytes(
          queryExecution.QueryExecution?.Statistics?.DataScannedInBytes || 0
        )} scanned`,
      }],
      results: resultSet,
      query: query,
      requestId: opt.requestId,
      pageSize: opt.limit,
      page: opt.page || 1,
    }];
  }
}
