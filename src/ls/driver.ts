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

    return this.connection;
  }
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
    const db = await this.connection;

    switch (item.childType) {
      case ContextValue.SCHEMA:
        const catalogs = await db.listDataCatalogs();

        return catalogs.DataCatalogsSummary.map((catalog) => ({
          database: "",
          label: catalog.CatalogName,
          type: item.childType,
          schema: catalog.CatalogName,
          childType: ContextValue.DATABASE,
        }));
      case ContextValue.DATABASE:
        let databaseList = [];
        let firstBatch: boolean = true;
        let nextToken: string | null = null;

        while (firstBatch == true || nextToken !== null) {
          firstBatch = false;
          let listDbRequest = {
            CatalogName: parent.schema,
          };
          if (nextToken !== null) {
            Object.assign(listDbRequest, {
              NextToken: nextToken,
            });
          }
          const catalog = await db.listDatabases(listDbRequest);
          nextToken = "NextToken" in catalog ? catalog.NextToken : null;

          databaseList = databaseList.concat(
            catalog.DatabaseList.map((database) => ({
              database: database.Name,
              label: database.Name,
              type: item.childType,
              schema: parent.schema,
              childType: ContextValue.TABLE,
            }))
          );
        }
        return databaseList;
      case ContextValue.TABLE:
        const tables = await this.rawQuery(
          `SHOW TABLES IN \`${parent.database}\``
        );
        const views = await this.rawQuery(`SHOW VIEWS IN "${parent.database}"`);

        const tableResults = await this.getQueryResults(
          tables.QueryExecution?.QueryExecutionId || ""
        );
        const viewResults = await this.getQueryResults(
          views.QueryExecution?.QueryExecutionId || ""
        );

        const tableRows = tableResults?.[0]?.ResultSet?.Rows || [];
        const viewRows = viewResults?.[0]?.ResultSet?.Rows || [];

        const viewsSet = new Set(
          viewRows.map((row) => row.Data[0].VarCharValue)
        );

        return tableRows
          .filter((row) => !viewsSet.has(row.Data[0].VarCharValue))
          .map((row) => ({
            database: parent.database,
            label: row.Data[0].VarCharValue,
            type: item.childType,
            schema: parent.schema,
            childType: ContextValue.COLUMN,
          }));
      case ContextValue.VIEW:
        const views2 = await this.rawQuery(
          `SHOW VIEWS IN "${parent.database}"`
        );
        const viewResults2 = await this.getQueryResults(
          views2.QueryExecution?.QueryExecutionId || ""
        );

        return viewResults2[0].ResultSet.Rows.map((row) => ({
          database: parent.database,
          label: row.Data[0].VarCharValue,
          type: item.childType,
          schema: parent.schema,
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
    switch (itemType) {
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        let j = 0;
        return [
          {
            database: "fakedb",
            label: `${search || "table"}${j++}`,
            type: itemType,
            schema: "fakeschema",
            childType: ContextValue.COLUMN,
          },
          {
            database: "fakedb",
            label: `${search || "table"}${j++}`,
            type: itemType,
            schema: "fakeschema",
            childType: ContextValue.COLUMN,
          },
          {
            database: "fakedb",
            label: `${search || "table"}${j++}`,
            type: itemType,
            schema: "fakeschema",
            childType: ContextValue.COLUMN,
          },
        ];
      case ContextValue.COLUMN:
        let i = 0;
        return [
          {
            database: "fakedb",
            label: `${search || "porra"}${i++}`,
            type: ContextValue.COLUMN,
            dataType: "faketype",
            schema: "fakeschema",
            childType: ContextValue.NO_CHILD,
            isNullable: false,
            iconName: "column",
            table: "fakeTable",
          },
          {
            database: "fakedb",
            label: `${search || "column"}${i++}`,
            type: ContextValue.COLUMN,
            dataType: "faketype",
            schema: "fakeschema",
            childType: ContextValue.NO_CHILD,
            isNullable: false,
            iconName: "column",
            table: "fakeTable",
          },
          {
            database: "fakedb",
            label: `${search || "column"}${i++}`,
            type: ContextValue.COLUMN,
            dataType: "faketype",
            schema: "fakeschema",
            childType: ContextValue.NO_CHILD,
            isNullable: false,
            iconName: "column",
            table: "fakeTable",
          },
          {
            database: "fakedb",
            label: `${search || "column"}${i++}`,
            type: ContextValue.COLUMN,
            dataType: "faketype",
            schema: "fakeschema",
            childType: ContextValue.NO_CHILD,
            isNullable: false,
            iconName: "column",
            table: "fakeTable",
          },
          {
            database: "fakedb",
            label: `${search || "column"}${i++}`,
            type: ContextValue.COLUMN,
            dataType: "faketype",
            schema: "fakeschema",
            childType: ContextValue.NO_CHILD,
            isNullable: false,
            iconName: "column",
            table: "fakeTable",
          },
        ];
    }
    return [];
  }

  public async getStaticCompletions(): Promise<{ [w: string]: NSDatabase.IStaticCompletion }> {
    const queryExecution = await this.rawQuery('SHOW FUNCTIONS');
    const results = await this.getQueryResults(queryExecution.QueryExecution?.QueryExecutionId || '');
    
    const functionDetails: { [w: string]: NSDatabase.IStaticCompletion } = {};
    
    // Process function results
    results.forEach(result => {
      result.ResultSet.Rows.forEach(row => {
        if (!row.Data?.[0]?.VarCharValue) return;
        
        const functionName = row.Data[0].VarCharValue;
        const returnType = row.Data[1]?.VarCharValue || '';
        const argumentTypes = row.Data[2]?.VarCharValue || '';
        // const functionType = row.Data[3]?.VarCharValue || '';
        const description = row.Data[5]?.VarCharValue || '';
        
        functionDetails[functionName] = {
          label: functionName,
          detail: `${functionName}(${argumentTypes}) → ${returnType}`,
          documentation: { 
            kind: 'markdown',
            value: description
          }
        };
      });
    });

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

    return { ...functionDetails, ...keywords };
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
