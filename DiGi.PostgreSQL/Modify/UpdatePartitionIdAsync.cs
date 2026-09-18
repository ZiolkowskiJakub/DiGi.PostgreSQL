using Npgsql;
using System.Threading;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Modify
    {
        /// <summary>
        /// Updates or creates a partition ID based on the provided name and data type.
        /// </summary>
        /// <param name="npgsqlConnection">The Npgsql connection to use for database operations.</param>
        /// <param name="name">The name of the partition.</param>
        /// <param name="dataType">The data type associated with the partition.</param>
        /// <param name="commandTimeout">The timeout in seconds for the execution of the command. A value of 0 disables the timeout.</param>
        /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the updated or created <see cref="Classes.Partition"/> object, or <see langword="null"/> if the operation failed.</returns>
        public static async Task<Classes.Partition?> UpdatePartitionIdAsync(this NpgsqlConnection? npgsqlConnection, string? name, Enums.DataType dataType, int commandTimeout = 30, CancellationToken cancellationToken = default)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            Classes.Partition? partition = await Query.PartitionAsync(npgsqlConnection, name, commandTimeout: commandTimeout, cancellationToken: cancellationToken);
            if (partition is null)
            {
                string commandText = @"
                INSERT INTO partitions (name, data_type)
                VALUES (@name, @data_type)
                RETURNING id;";

                await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
                npgsqlCommand.CommandTimeout = commandTimeout;
                npgsqlCommand.Parameters.AddWithValue("name", name);
                npgsqlCommand.Parameters.AddWithValue("data_type", (short)dataType);

                short? partitionId = (short?)await npgsqlCommand.ExecuteScalarAsync(cancellationToken);
                if (partitionId is null)
                {
                    return null;
                }

                partition = await Query.PartitionAsync(npgsqlConnection, name, commandTimeout: commandTimeout, cancellationToken: cancellationToken);
            }

            if (partition is null)
            {
                return null;
            }

            bool created = await Create.TableAsync_Objects_Partition(npgsqlConnection, dataType, partition.Id, commandTimeout: commandTimeout, cancellationToken: cancellationToken);
            if (created)
            {
                return partition;
            }

            return null;
        }
    }
}