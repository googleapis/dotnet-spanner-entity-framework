// Copyright 2021, Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Cloud.Spanner.Data;
using Grpc.Core;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Extensions.Internal
{
    /// <summary>
    /// ModelValidationConnectionProvider is registered as a singleton service that provides a
    /// connection to the Spanner database that is used with Entity Framework. This provider will only
    /// return a connection as long as the application is only connecting to one database. If the
    /// application uses Entity Framework to connect to multiple different Spanner databases, the provider will
    /// return null.
    /// 
    /// The connection that is returned by this provider can be used by the SpannerModelValidator
    /// to check the entity model that is used against the database to check for common misconfigurations.
    /// 
    /// Call <see cref="EnableDatabaseModelValidation(bool)"/> to disable
    /// model validation against the actual database.
    /// </summary>
    public class SpannerModelValidationConnectionProvider
    {
        /// <summary>
        /// The singleton instance of the connection provider for model validation.
        /// </summary>
        public static readonly SpannerModelValidationConnectionProvider Instance = new SpannerModelValidationConnectionProvider();

        private readonly object _lck = new object();
        private bool _enabled = true;
        private string _connectionString;
        private ChannelCredentials _channelCredentials;
        private SpannerConnectionStringBuilder _connectionStringBuilder;

        private SpannerModelValidationConnectionProvider()
        {
        }

        /// <summary>
        /// Enables or disables model validation against an actual database. Model validation is automatically
        /// disabled during migrations that are executed through Entity Framework. Disable model validation
        /// manually if you are experiencing validation failures during manual DDL updates or other manual
        /// migrations of your database.
        /// </summary>
        /// <param name="enable"></param>
        public void EnableDatabaseModelValidation(bool enable)
        {
            lock (_lck)
            {
                _enabled = enable;
            }
        }

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// 
        /// Resets the connection provider to its initial state. This will (re-)enable model validation against
        /// an actual database. Calling this in a multi-threaded environment that accesses multiple different Spanner
        /// databases can cause random validation errors.
        /// </summary>
        public void Reset()
        {
            lock (_lck)
            {
                _connectionString = null;
                _channelCredentials = null;
                _connectionStringBuilder = null;
            }
        }

        internal SpannerConnection GetConnection()
        {
            lock (_lck)
            {
                return !_enabled || _connectionString == null ? null : new SpannerConnection(_connectionString, _channelCredentials);
            }
        }

        internal void SetConnectionString(string value, ChannelCredentials channelCredentials = null)
        {
            lock (_lck)
            {
                if (_connectionString == null && _connectionStringBuilder != null)
                {
                    // This means that the provider has already seen at least two different
                    // connection strings.
                    return;
                }

                var builder = new SpannerConnectionStringBuilder(value);
                // Ignore connection strings without a valid database.
                if (builder.DatabaseName == null)
                {
                    return;
                }
                // Check if this is the first valid connection string that has been set.
                // If so, register this as THE connection string and use that for validation.
                if (_connectionString == null && _connectionStringBuilder == null)
                {
                    // This is the first valid connection string.
                    _connectionStringBuilder = builder;
                    _connectionString = value;
                    _channelCredentials = channelCredentials;
                    return;
                }
                // This is not the first valid connection string. Check whether the new
                // connection string is connecting to the same database. If it is, it is
                // still safe to do validation against the database.
                if (!builder.DatabaseName.Equals(_connectionStringBuilder?.DatabaseName))
                {
                    // The application is connecting to a new database. It is no longer safe
                    // to validate against the database, as there is no guarantee that the
                    // order of validation will be equal to the order of calling UseSpanner(connectionString).
                    _connectionString = null;
                    _channelCredentials = null;
                }
            }
        }
    }
}
