using DiGi.PostgreSQL.Interfaces;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.Classes
{
    public class PostgreSQLConverterManager<TPostgreSQLConverter> : IPostgreSQLObject where TPostgreSQLConverter : IPostgreSQLConverter
    {
        private readonly List<Tuple<TPostgreSQLConverter, PostgreSQLConfigurationFile?>> tuples = [];

        public PostgreSQLConverterManager()
        {
        }

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

        public PostgreSQLConfigurationFile? GetPostgreSQLConfigurationFile<UPostgreSQLConverter>() where UPostgreSQLConverter : TPostgreSQLConverter
        {
            if (!TryGetPostgreSQLConfigurationFile<UPostgreSQLConverter>(out PostgreSQLConfigurationFile? result))
            {
                return default;
            }

            return result;
        }

        public UPostgreSQLConverter? GetPostgreSQLConverter<UPostgreSQLConverter>() where UPostgreSQLConverter : TPostgreSQLConverter
        {
            if (!TryGetPostgreSQLConverter(out UPostgreSQLConverter? result))
            {
                return default;
            }

            return result;
        }

        public async Task<bool> TryCreateDatabase<UPostgreSQLConverter>() where UPostgreSQLConverter : TPostgreSQLConverter
        {
            if (!TryGetPostgreSQLConfigurationFile<UPostgreSQLConverter>(out PostgreSQLConfigurationFile? postgreSQLConfigurationFile) || postgreSQLConfigurationFile is null)
            {
                return false;
            }

            return await Create.DatabaseAsync(postgreSQLConfigurationFile);
        }

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

    public class PostgreSQLConverterManager : PostgreSQLConverterManager<IPostgreSQLConverter>
    {
        public PostgreSQLConverterManager()
        {
        }
    }
}