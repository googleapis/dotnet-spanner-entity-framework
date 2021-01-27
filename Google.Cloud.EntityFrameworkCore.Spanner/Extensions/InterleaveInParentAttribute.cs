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

using Microsoft.EntityFrameworkCore.Metadata;

namespace System.ComponentModel.DataAnnotations
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class InterleaveInParentAttribute : Attribute
    {
        /// <summary>
        /// The parent entity that the child will be interleaved with.
        /// Must be a valid entity type.
        /// </summary>
        public Type ParentEntity { get; set; }

        /// <summary>
        /// Action on delete operation, default is `No Action`.
        /// </summary>
        public OnDelete OnDelete { get; set; }

        public InterleaveInParentAttribute(Type parentEntity, OnDelete onDelete = OnDelete.NoAction)
        {
            ParentEntity = parentEntity;
            OnDelete = onDelete;
        }
    }
}
