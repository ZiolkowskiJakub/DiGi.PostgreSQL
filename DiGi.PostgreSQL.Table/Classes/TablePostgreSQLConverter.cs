using DiGi.Core.IO.Table.Classes;
using DiGi.Core.IO.Table.Interfaces;
using DiGi.PostgreSQL.Classes;
using Npgsql;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.Table.Classes
{
    public abstract class TablePostgreSQLConverter<UColumn> : PostgreSQLConverter<Table<UColumn>> where UColumn : IColumn
    {
        public virtual TableConversionOptions<UColumn>? TableConversionOptions { get; } = null;

        public abstract string TableName { get; }

        public TablePostgreSQLConverter(ConnectionData? connectionData)
            : base(connectionData)
        {
        }

        public async Task<bool> UpdateAsync<TColumn>(Table<TColumn>? table) where TColumn : UColumn
        {
            if(table is null)
            {
                return false;
            }

            if(table.Columns is not IEnumerable<TColumn> columns)
            {
                return false;
            }

            if(table.RowCount == 0)
            {
                return false;
            }

            await using NpgsqlConnection? npgsqlConnection = PostgreSQL.Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return false;
            }

            npgsqlConnection.Open();

            await CreateTableAsync(npgsqlConnection, columns.Cast<UColumn>());


            throw new System.NotImplementedException();
            
        }



        private async Task<bool> CreateTableAsync(NpgsqlConnection? npgsqlConnection, IEnumerable<UColumn> columns)
        {
            return await Create.TableAsync(npgsqlConnection, TableName, TableConversionOptions, columns);
        }
    }
}