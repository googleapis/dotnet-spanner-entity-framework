// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.TestUtilities
{
    public abstract class TestHelpers
    {
        public IServiceProvider CreateServiceProvider(IServiceCollection customServices = null)
            => CreateServiceProvider(customServices, AddProviderServices);

        private static IServiceProvider CreateServiceProvider(
            IServiceCollection customServices,
            Func<IServiceCollection, IServiceCollection> addProviderServices)
        {
            var services = new ServiceCollection();
            addProviderServices(services);

            if (customServices != null)
            {
                foreach (var service in customServices)
                {
                    services.Add(service);
                }
            }

            return services.BuildServiceProvider();
        }

        public abstract IServiceCollection AddProviderServices(IServiceCollection services);

        public DbContextOptionsBuilder AddProviderOptions(DbContextOptionsBuilder optionsBuilder)
        {
            UseProviderOptions(optionsBuilder);
            return optionsBuilder;
        }

        public ModelBuilder CreateConventionBuilder(bool skipValidation = false)
        {
            var conventionSet = CreateContextServices().GetRequiredService<IConventionSetBuilder>()
                .CreateConventionSet();

            if (skipValidation)
            {
                ConventionSet.Remove(conventionSet.ModelFinalizedConventions, typeof(ValidatingConvention));
            }

            return new ModelBuilder(conventionSet);
        }

        public IServiceProvider CreateContextServices()
            => ((IInfrastructure<IServiceProvider>)CreateContext()).Instance;

        public DbContext CreateContext()
            => new DbContext(CreateOptions(CreateServiceProvider()));

        public DbContextOptions CreateOptions(IServiceProvider serviceProvider = null)
        {
            var optionsBuilder = new DbContextOptionsBuilder()
                .UseInternalServiceProvider(serviceProvider);

            UseProviderOptions(optionsBuilder);

            return optionsBuilder.Options;
        }

        protected abstract void UseProviderOptions(DbContextOptionsBuilder optionsBuilder);
    }
}