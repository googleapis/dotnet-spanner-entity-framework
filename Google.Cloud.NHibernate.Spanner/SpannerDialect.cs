using NHibernate;
using NHibernate.Dialect;
using NHibernate.Dialect.Function;
using NHibernate.Dialect.Schema;
using NHibernate.SqlCommand;
using System.Data;
using System.Data.Common;
using Environment = NHibernate.Cfg.Environment;

namespace Google.Cloud.NHibernate.Spanner
{
    public class SpannerDialect : Dialect
    {
        public SpannerDialect()
        {
			DefaultProperties[Environment.ConnectionDriver] = typeof(SpannerDriver).AssemblyQualifiedName;

			RegisterDateTimeTypeMappings();
			RegisterColumnType(DbType.AnsiStringFixedLength, "STRING(MAX)");
			RegisterColumnType(DbType.AnsiStringFixedLength, 8000, "STRING($l)");
			RegisterColumnType(DbType.AnsiString, "STRING(MAX)");
			RegisterColumnType(DbType.AnsiString, 8000, "STRING($l)");
			RegisterColumnType(DbType.AnsiString, 2147483647, "STRING(MAX)");
			RegisterColumnType(DbType.Binary, "BYTES(MAX)");
			RegisterColumnType(DbType.Binary, 2147483647, "BYTES(MAX)");
			RegisterColumnType(DbType.Boolean, "BOOL");
			RegisterColumnType(DbType.Byte, "INT64");
			RegisterColumnType(DbType.Currency, "NUMERIC");
			RegisterColumnType(DbType.Decimal, "NUMERIC");
			RegisterColumnType(DbType.Double, "FLOAT64");
			RegisterColumnType(DbType.Int16, "INT64");
			RegisterColumnType(DbType.Int32, "INT64");
			RegisterColumnType(DbType.Int64, "INT64");
			RegisterColumnType(DbType.Single, "FLOAT64");
			RegisterColumnType(DbType.StringFixedLength, "STRING(MAX)");
			RegisterColumnType(DbType.StringFixedLength, 4000, "STRING($l)");
			RegisterColumnType(DbType.String, "STRING(MAX)");
			RegisterColumnType(DbType.String, 4000, "STRING($l)");
			RegisterColumnType(DbType.String, 1073741823, "STRING(MAX)");

			// Override standard HQL function
			RegisterFunction("current_timestamp", new NoArgSQLFunction("CURRENT_TIMESTAMP", NHibernateUtil.LocalDateTime, true));
			RegisterFunction("str", new SQLFunctionTemplate(NHibernateUtil.String, "cast(?1 as STRING)"));

			RegisterKeywords();
		}

		#region private static readonly string[] DialectKeywords = { ... }

		private static readonly string[] DialectKeywords =
		{
		};

		#endregion

		protected virtual void RegisterDateTimeTypeMappings()
		{
			RegisterColumnType(DbType.Date, "DATE");
			RegisterColumnType(DbType.DateTime, "TIMESTAMP");
			RegisterColumnType(DbType.Time, "TIMESTAMP");
		}

		protected virtual void RegisterKeywords()
		{
			RegisterKeywords(DialectKeywords);
		}

		public override string AddColumnString => "ADD COLUMN";

		public override bool SupportsLimit => true;

		public override bool SupportsLimitOffset => true;

		public override SqlString GetLimitString(SqlString queryString, SqlString offset, SqlString limit)
		{
			SqlStringBuilder pagingBuilder = new SqlStringBuilder();
			pagingBuilder.Add(queryString);

			if (limit != null)
			{
				pagingBuilder.Add(" LIMIT ");
				pagingBuilder.Add(limit);
			}

			if (offset != null)
			{
				// TODO: OFFSET is only supported in combination with LIMIT
				pagingBuilder.Add(" OFFSET ");
				pagingBuilder.Add(offset);
			}

			return pagingBuilder.ToSqlString();
		}

		public override bool SupportsUnionAll => true;

		public override string ToBooleanValueString(bool value)
			=> value ? "TRUE" : "FALSE";
		
		public override IDataBaseSchema GetDataBaseSchema(DbConnection connection)
		{
			// TODO: Replace with Spanner specific metadata.
			return new PostgreSQLDataBaseMetadata(connection);
		}

		public override bool SupportsCurrentTimestampSelection => true;

		public override string CurrentTimestampSelectString => "SELECT CURRENT_TIMESTAMP";

		#region Overridden informational metadata

		public override bool SupportsEmptyInList => false;

		public override bool UseInputStreamToInsertBlob => false;

		public override bool SupportsLobValueChangePropogation => false;

		public override bool SupportsUnboundedLobLocatorMaterialization => false;

		public override bool SupportsDistributedTransactions => false;

		#endregion
    }
}