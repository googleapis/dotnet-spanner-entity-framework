// Copyright 2021 Google Inc. All Rights Reserved.
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

using Docker.DotNet;
using Docker.DotNet.Models;
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Samples
{
    /// <summary>
    /// This class can be used to programmatically start and stop an instance of the Cloud Spanner emulator.
    /// </summary>
    internal class EmulatorRunner
    {
        private static readonly string s_emulatorImageName = "gcr.io/cloud-spanner-emulator/emulator";
        private readonly DockerClient _dockerClient;
        private string _containerId;

        public EmulatorRunner()
        {
            _dockerClient = new DockerClientConfiguration(new Uri(GetDockerApiUri())).CreateClient();
        }

        /// <summary>
        /// Downloads the latest Spanner emulator docker image and starts the emulator on port 9010.
        /// </summary>
        internal async Task StartEmulator()
        {
            await PullEmulatorImage();
            var response = await _dockerClient.Containers.CreateContainerAsync(new CreateContainerParameters
            {
                Image = s_emulatorImageName,
                ExposedPorts = new Dictionary<string, EmptyStruct>
                {
                    {
                        "9010", default
                    }
                },
                HostConfig = new HostConfig
                {
                    PortBindings = new Dictionary<string, IList<PortBinding>>
                    {
                        {"9010", new List<PortBinding> {new PortBinding {HostPort = "9010"}}}
                    },
                    PublishAllPorts = true
                }
            });
            _containerId = response.ID;
            await _dockerClient.Containers.StartContainerAsync(_containerId, null);
        }

        /// <summary>
        /// Stops the currently running emulator. Fails if no emulator has been started.
        /// </summary>
        internal async Task StopEmulator()
        {
            if (_containerId != null)
            {
                await _dockerClient.Containers.KillContainerAsync(_containerId, new ContainerKillParameters());
            }
        }

        private async Task PullEmulatorImage()
        {
            await _dockerClient.Images.CreateImageAsync(new ImagesCreateParameters
            {
                FromImage = s_emulatorImageName,
                Tag = "latest"
            },
                new AuthConfig(),
                new Progress<JSONMessage>());
        }

        private string GetDockerApiUri()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                return "npipe://./pipe/docker_engine";
            }
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) || RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                return "unix:/var/run/docker.sock";
            }
            throw new Exception("Was unable to determine what OS this is running on, does not appear to be Windows or Linux!?");
        }
    }
}
