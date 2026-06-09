using DiGi.PostgreSQL.Interfaces;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.Classes
{
    /// <summary>
    /// Manages a collection of PostgreSQL converters and their associated configuration files.
    /// </summary>
    /// <typeparam name="TPostgreSQLConverter">The base type of the PostgreSQL converter to be managed.</typeparam>
    public class PostgreSQLConverterManager<TPostgreSQLConverter> : IPostgreSQLObject where TPostgreSQLConverter : IPostgreSQLConverter
    {
        private readonly List<Tuple<TPostgreSQLConverter, PostgreSQLConfigurationFile?>> tuples = [];

        /// <summary>
        /// Initializes a new instance of the <see cref="PostgreSQLConverterManager{TPostgreSQLConverter}"/> class.
        /// </summary>
        public PostgreSQLConverterManager()
        {
        }

        /// <summary>
        /// Adds or updates a PostgreSQL converter and its associated configuration file in the manager.
        /// </summary>
        /// <param name="postgreSQLConverter">The PostgreSQL converter to add.</param>
        /// <param name="postgreSQLConfigurationFile">The optional configuration file associated with the converter.</param>
        /// <returns>True if the converter was successfully added or updated; otherwise, false.</returns>
        public bool Add(TPostgreSQLConverter? postgreSQLConverter, PostgreSQLConfigurationFile? postgreSQLConfigurationFile = null)
        {
            if (postgreSQLConverter is null)
            {
                return false;
            }

            int index = tuples.FindIndex(x => x.Item1.GetType() == postgreSQLConverter.GetType());
            if (index == -1)
            {
                tuples.Add(new Tuple<TPostgreSQLConverter, PostgreSQLConfigurationFile?>(postgreSQLConverter, postgreSQLConfigurationFile));
            }
            else
            {
                tuples[index] = new Tuple<TPostgreSQLConverter, PostgreSQLConfigurationFile?>(postgreSQLConverter, postgreSQLConfigurationFile);
            }

            return true;
        }

        /// <summary>
        /// Retrieves the configuration file for a specific type of PostgreSQL converter.
        /// </summary>
        /// <typeparam name="UPostgreSQLConverter">The specific type of the PostgreSQL converter.</typeparam>
        /// <returns>The associated <see cref="PostgreSQLConfigurationFile"/>, or null if not found.</returns>
        public PostgreSQLConfigurationFile? GetPostgreSQLConfigurationFile<UPostgreSQLConverter>() where UPostgreSQLConverter : TPostgreSQLConverter
        {
            if (!TryGetPostgreSQLConfigurationFile<UPostgreSQLConverter>(out PostgreSQLConfigurationFile? result))
            {
                return default;
            }

            return result;
        }

        /// <summary>
        /// Retrieves a specific instance of a PostgreSQL converter by its type.
        /// </summary>
        /// <typeparam name="UPostgreSQLConverter">The specific type of the PostgreSQL converter to retrieve.</typeparam>
        /// <returns>The <typeparamref name="UPostgreSQLConverter"/> instance, or null if not found.</returns>
        public UPostgreSQLConverter? GetPostgreSQLConverter<UPostgreSQLConverter>() where UPostgreSQLConverter : TPostgreSQLConverter
        {
            if (!TryGetPostgreSQLConverter(out UPostgreSQLConverter? result))
            {
                return default;
            }

            return result;
        }

        /// <summary>
        /// Retrieves all registered converters of a specific type.
        /// </summary>
        /// <typeparam name="UPostgreSQLConverter">The specific type of the PostgreSQL converters to retrieve.</typeparam>
        /// <returns>A list of <typeparamref name="UPostgreSQLConverter"/> instances.</returns>
        public List<UPostgreSQLConverter> GetPostgreSQLConverters<UPostgreSQLConverter>() where UPostgreSQLConverter : TPostgreSQLConverter
        {
            List<UPostgreSQLConverter> result = [];

            foreach (Tuple<TPostgreSQLConverter, PostgreSQLConfigurationFile?> tuple in tuples)
            {
                if (tuple.Item1 is UPostgreSQLConverter postgreSQLConverter)
                {
                    result.Add(postgreSQLConverter);
                }
            }

            return result;
        }

        /// <summary>
        /// Checks if the database associated with a specific converter type is available.
        /// </summary>
        /// <typeparam name="UPostgreSQLConverter">The specific type of the PostgreSQL converter.</typeparam>
        /// <returns>True if the configuration exists and the database is available; otherwise, false.</returns>
        public bool IsAvailable<UPostgreSQLConverter>() where UPostgreSQLConverter : TPostgreSQLConverter
        {
            if (!TryGetPostgreSQLConfigurationFile<UPostgreSQLConverter>(out PostgreSQLConfigurationFile? postgreSQLConfigurationFile) || postgreSQLConfigurationFile is null)
            {
                return false;
            }
            return Query.IsAvailable(postgreSQLConfigurationFile);
        }

        /// <summary>
        /// Attempts to create a database using the configuration associated with a specific converter type.
        /// </summary>
        /// <typeparam name="UPostgreSQLConverter">The specific type of the PostgreSQL converter.</typeparam>
        /// <returns>A task representing the asynchronous operation, containing true if the database was created successfully; otherwise, false.</returns>
        public async Task<bool> TryCreateDatabase<UPostgreSQLConverter>() where UPostgreSQLConverter : TPostgreSQLConverter
        {
            if (!TryGetPostgreSQLConfigurationFile<UPostgreSQLConverter>(out PostgreSQLConfigurationFile? postgreSQLConfigurationFile) || postgreSQLConfigurationFile is null)
            {
                return false;
            }

            return await Create.DatabaseAsync(postgreSQLConfigurationFile);
        }

        /// <summary>
        /// Attempts to retrieve the configuration file for a specific converter type.
        /// </summary>
        /// <typeparam name="UPostgreSQLConverter">The specific type of the PostgreSQL converter.</typeparam>
        /// <param name="postgreSQLConfigurationFile">When this method returns, contains the configuration file if found; otherwise, null.</param>
        /// <returns>True if the configuration file was successfully retrieved; otherwise, false.</returns>
        public bool TryGetPostgreSQLConfigurationFile<UPostgreSQLConverter>(out PostgreSQLConfigurationFile? postgreSQLConfigurationFile) where UPostgreSQLConverter : TPostgreSQLConverter
        {
            postgreSQLConfigurationFile = default;

            int index = tuples.FindIndex(x => x.Item1.GetType() == typeof(UPostgreSQLConverter));
            if (index == -1)
            {
                return false;
            }

            if (tuples[index].Item2 is not PostgreSQLConfigurationFile result)
            {
                return false;
            }

            postgreSQLConfigurationFile = result;
            return true;
        }

        /// <summary>
        /// Attempts to retrieve a converter instance of a specific type.
        /// </summary>
        /// <typeparam name="UPostgreSQLConverter">The specific type of the PostgreSQL converter.</typeparam>
        /// <param name="postgreSQLConverter">When this method returns, contains the converter instance if found; otherwise, null.</param>
        /// <returns>True if the converter was successfully retrieved; otherwise, false.</returns>
        public bool TryGetPostgreSQLConverter<UPostgreSQLConverter>(out UPostgreSQLConverter? postgreSQLConverter) where UPostgreSQLConverter : TPostgreSQLConverter
        {
            int index = tuples.FindIndex(x => x.Item1.GetType() == typeof(UPostgreSQLConverter));
            if (index == -1)
            {
                postgreSQLConverter = default;
                return false;
            }

            postgreSQLConverter = (UPostgreSQLConverter)(object)tuples[index].Item1;
            return true;
        }
    }

    /// <summary>
    /// A non-generic manager for PostgreSQL converters using the <see cref="IPostgreSQLConverter"/> interface.
    /// </summary>
    public class PostgreSQLConverterManager : PostgreSQLConverterManager<IPostgreSQLConverter>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PostgreSQLConverterManager"/> class.
        /// </summary>
        public PostgreSQLConverterManager()
        {
        }
    }
}