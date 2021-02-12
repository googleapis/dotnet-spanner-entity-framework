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

using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.TestUtilities
{
    public class TestServiceFactory
    {
        public static readonly TestServiceFactory Instance = new TestServiceFactory();

        private TestServiceFactory()
        {
        }

        private readonly ConcurrentDictionary<System.Type, IServiceProvider> _factories
            = new ConcurrentDictionary<System.Type, IServiceProvider>();

        public TService Create<TService>(params (System.Type Type, object Implementation)[] specialCases)
            where TService : class
        {
            return _factories.GetOrAdd(
                typeof(TService),
                t => AddType(new ServiceCollection(), typeof(TService), specialCases).BuildServiceProvider()).GetService<TService>();
        }

        private static ServiceCollection AddType(
            ServiceCollection serviceCollection,
            System.Type serviceType,
            IList<(System.Type Type, object Implementation)> specialCases)
        {
            var implementation = specialCases.Where(s => s.Type == serviceType).Select(s => s.Implementation).FirstOrDefault();

            if (implementation != null)
            {
                serviceCollection.AddSingleton(serviceType, implementation);
            }
            else
            {
                foreach (var (ServiceType, ImplementationType) in GetImplementationType(serviceType))
                {
                    implementation = specialCases.Where(s => s.Type == ImplementationType).Select(s => s.Implementation).FirstOrDefault();

                    if (implementation != null)
                    {
                        serviceCollection.AddSingleton(ServiceType, implementation);
                    }
                    else
                    {
                        serviceCollection.AddSingleton(ServiceType, ImplementationType);

                        var constructors = ImplementationType.GetConstructors();
                        var constructor = constructors
                            .FirstOrDefault(c => c.GetParameters().Length == constructors.Max(c2 => c2.GetParameters().Length));

                        if (constructor == null)
                        {
                            throw new InvalidOperationException(
                                $"Cannot use 'TestServiceFactory' for '{ImplementationType.ShortDisplayName()}': no public constructor.");
                        }

                        foreach (var parameter in constructor.GetParameters())
                        {
                            AddType(serviceCollection, parameter.ParameterType, specialCases);
                        }
                    }
                }
            }

            return serviceCollection;
        }

        private static IList<(System.Type ServiceType, System.Type ImplementationType)> GetImplementationType(System.Type serviceType)
        {
            if (!serviceType.IsInterface)
            {
                return new[] { (serviceType, serviceType) };
            }

            var elementType = TryGetEnumerableType(serviceType);

            var implementationTypes = (elementType ?? serviceType)
                .Assembly
                .GetTypes()
                .Where(t => (elementType ?? serviceType).IsAssignableFrom(t) && !t.IsAbstract)
                .ToList();

            if (elementType == null)
            {
                if (implementationTypes.Count != 1)
                {
                    throw new InvalidOperationException(
                        $"Cannot use 'TestServiceFactory' for '{serviceType.ShortDisplayName()}': no single implementation type in same assembly.");
                }

                return new[] { (serviceType, implementationTypes[0]) };
            }

            return implementationTypes.Select(t => (elementType, t)).ToList();
        }

        private static System.Type TryGetEnumerableType(System.Type type)
            => !type.GetTypeInfo().IsGenericTypeDefinition
            && type.GetTypeInfo().IsGenericType
            && type.GetGenericTypeDefinition() == typeof(IEnumerable<>)
            ? type.GetTypeInfo().GenericTypeArguments[0] : null;
    }
}
