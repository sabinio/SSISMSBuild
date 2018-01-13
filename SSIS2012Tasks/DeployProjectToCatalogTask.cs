using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using Microsoft.Build.Framework;
using Microsoft.Build.Utilities;

namespace Microsoft.SqlServer.IntegrationServices.Build
{
	/// <summary>
	/// This Task connects to an SSIS Catalog and deploys the given project files.
	/// Ensure that the account running MSBuild has permission to deploy to the catalog.
	/// </summary>
	public class DeployProjectToCatalogTask : Task
	{
		/// <summary>
		/// One or more paths to .ispac deployment files.
		/// </summary>
		[Required]
		public ITaskItem[] DeploymentFile { get; set; }

		/// <summary>
		/// The SQL instance name of the SSIS Catalog to deploy to.
		/// </summary>
		[Required]
		public string Instance { get; set; }

		/// <summary>
		/// The folder on the catalog to deploy to.
		/// If this folder does not exist, it will be created if <see cref="CreateFolder"/> is true.
		/// </summary>
		[Required]
		public string Folder { get; set; }

		/// <summary>
		/// Should the SSIS Catalog Folder be created if it is not already there. 
		/// This property is optional. The default value is true.
		/// </summary>
		public bool CreateFolder { get; set; }


        /// <summary>
        /// The Environment reference for configuration.
        /// </summary>
        [Required]
        public string Environment { get; set; }

        /// <summary>
        /// Gets or sets the wait time before terminating the attempt to execute a command and generating an error.
        /// </summary>

        public int SqlCommandTimeout { get; set; }


        /// <summary>
		/// The name of the SSIS catalog to deploy to.
		/// This property is optional. The default value is "SSISDB".
		/// </summary>
		public string Catalog { get; set; }

		public DeployProjectToCatalogTask()
		{
			Catalog = "SSISDB";
			CreateFolder = true;
            SqlCommandTimeout = 300;
		}

		public override bool Execute()
		{
			bool result = true;
			var csb = new SqlConnectionStringBuilder
			          	{
			          		DataSource = Instance, IntegratedSecurity = true, InitialCatalog = Catalog, ConnectTimeout = 600
			          	};

			Log.LogMessage(SR.ConnectingToServer, csb.ConnectionString);

			using (var conn = new SqlConnection(csb.ConnectionString))
			{
				try
				{
					conn.Open();
				}
				catch (Exception e)
				{
					Log.LogError(SR.ConnectionError);
					Log.LogErrorFromException(e);
					return false;
				}

				foreach (var taskItem in DeploymentFile)
				{
					try
					{
						Log.LogMessage("------");

						string projectPath = taskItem.ItemSpec;

						if (CreateFolder)
						{
							EnsureFolderExists(conn, Folder);
						}

                        this.EnsureEnvironmentExists(conn, Folder, Environment);

						string projectName = Path.GetFileNameWithoutExtension(projectPath);
						var bytes = File.ReadAllBytes(projectPath);

                        var deploymentCmd = GetDeploymentCommand(conn, Folder, projectName, bytes, SqlCommandTimeout);

						try
						{
							Log.LogMessage(SR.DeployingProject, projectPath);
							deploymentCmd.ExecuteNonQuery();
						}
						catch (Exception)
						{
							Log.LogError(SR.DeploymentFailed);
							throw;
						}

                        this.EnsureEnvironmentReferenceExists(conn, projectName, Folder, Environment);


					}
					catch (Exception e)
					{
						Log.LogErrorFromException(e, true);
						result = false;
					}
				}
			}

			return result;
		}

		private void EnsureFolderExists(SqlConnection connection, string folder)
		{
            if (!FolderExists(connection, folder, SqlCommandTimeout))
			{
				CreateCatalogFolder(connection, folder);
			}
		}

        private void EnsureEnvironmentExists(SqlConnection connection, string folder, string environment)
        {

            if (!EnvironmentExists(connection, folder, environment, SqlCommandTimeout))
            {
                CreateCatalogEnvironment(connection, folder, environment);
            }
        }

        private void EnsureEnvironmentReferenceExists(SqlConnection connection, string project, string folder, string environment)
        {

            if (!EnvironmentReferenceExists(connection, project, folder, environment, SqlCommandTimeout))
            {
                CreateCatalogEnvironmentReference(connection, project, folder, environment);
            }
        }

        private static bool FolderExists(SqlConnection connection, string folder, int SqlCommandTimeout)
        {
            var cmd = GetFolderCommand(connection, folder, SqlCommandTimeout);
            var folderId = cmd.ExecuteScalar();
            return (folderId != null && folderId != DBNull.Value);
        }


        private static bool EnvironmentExists(SqlConnection connection, string folder, string environment, int SqlCommandTimeout)
        {
            var cmd = GetEnvironmentCommand(connection, folder, environment, SqlCommandTimeout);
            var environmentId = cmd.ExecuteScalar();
            return (environmentId != null && environmentId != DBNull.Value);
        }

        private static bool EnvironmentReferenceExists(SqlConnection connection, string projectName, string folder, string environment, int SqlCommandTimeout)
        {
            var cmd = GetEnvironmentReferenceCommand(connection, projectName, folder, environment, SqlCommandTimeout);
            var environmentRefId = cmd.ExecuteScalar();
            return (environmentRefId != null && environmentRefId != DBNull.Value);
        }

        private void CreateCatalogFolder(SqlConnection connection, string folder)
		{
			var cmd = new SqlCommand("[catalog].[create_folder]", connection) {CommandType = CommandType.StoredProcedure};
            cmd.CommandTimeout = SqlCommandTimeout;
            cmd.CommandTimeout = SqlCommandTimeout;
			cmd.Parameters.AddWithValue("folder_name", folder);

			Log.LogMessage(SR.CreatingFolder, folder);
			cmd.ExecuteNonQuery();
		}

        private void CreateCatalogEnvironment(SqlConnection connection, string folder, string environment)
        {
            var cmd = new SqlCommand("[catalog].[create_environment]", connection) { CommandType = CommandType.StoredProcedure };
            cmd.CommandTimeout = SqlCommandTimeout;
            cmd.Parameters.AddWithValue("folder_name", folder);
            cmd.Parameters.AddWithValue("environment_name", environment);

            Log.LogMessage(SR.CreatingEnvironment, environment);
            cmd.ExecuteNonQuery();
        }

        private void CreateCatalogEnvironmentReference(SqlConnection connection, string project, string folder, string environment)
        {
            var cmd = new SqlCommand("[catalog].[create_environment_reference]", connection) { CommandType = CommandType.StoredProcedure };
            cmd.CommandTimeout = SqlCommandTimeout;
            cmd.Parameters.AddWithValue("folder_name", folder);
            cmd.Parameters.AddWithValue("environment_name", environment);
            cmd.Parameters.AddWithValue("project_name", project);
            cmd.Parameters.AddWithValue("reference_type", "R");
            cmd.Parameters.AddWithValue("reference_id", SqlDbType.BigInt).Direction = ParameterDirection.Output;

            Log.LogMessage(SR.CreatingEnvironmentReference, environment);
            cmd.ExecuteNonQuery();
        }


        private static SqlCommand GetFolderCommand(SqlConnection connection, string folder, int SqlCommandTimeout)
		{
			var cmd = new SqlCommand("SELECT folder_id FROM [catalog].[folders] WHERE name = @FolderName", connection);
            cmd.CommandTimeout = SqlCommandTimeout;
			cmd.Parameters.AddWithValue("@FolderName", folder);

			return cmd;
		}

        private static SqlCommand GetEnvironmentCommand(SqlConnection connection, string folder, string environment, int SqlCommandTimeout)
        {
            var cmd = new SqlCommand("select e.environment_id from catalog.environments e inner join catalog.folders f on f.folder_id = e.folder_id where e.name = @EnvironmentName AND f.name = @FolderName", connection);
            cmd.CommandTimeout = SqlCommandTimeout;
            cmd.Parameters.AddWithValue("@EnvironmentName", environment);
            cmd.Parameters.AddWithValue("@FolderName", folder);

            return cmd;
        }

        private static SqlCommand GetEnvironmentReferenceCommand(SqlConnection connection, string projectName, string folder, string environment, int SqlCommandTimeout)
        {
            var cmd = new SqlCommand("select * from catalog.environment_references e inner join catalog.projects p on p.project_id = e.project_id where e.environment_name = @EnvironmentName AND p.name = @ProjectName", connection);
            cmd.CommandTimeout = SqlCommandTimeout;
            cmd.Parameters.AddWithValue("@FolderName", folder);
            cmd.Parameters.AddWithValue("@EnvironmentName", environment);
            cmd.Parameters.AddWithValue("@ProjectName", projectName);

            return cmd;
        }

        private static SqlCommand GetDeploymentCommand(SqlConnection connection, string folder, string name, byte[] project, int SqlCommandTimeout)
		{
			// build the deployment command
			var cmd = new SqlCommand("[catalog].[deploy_project]", connection) { CommandType = CommandType.StoredProcedure };
            cmd.CommandTimeout = SqlCommandTimeout;
			cmd.Parameters.AddWithValue("folder_name", folder);
			cmd.Parameters.AddWithValue("project_name", name);
			cmd.Parameters.AddWithValue("project_stream", project);
			cmd.Parameters.AddWithValue("operation_id", SqlDbType.BigInt).Direction = ParameterDirection.Output;

			return cmd;
		}
	}
}
