package main

/* base mapping
 * repo shortname to repo fullname
 * and repo_name to database name
 */

// map short repo names to full repository names
func map_repo_names(name string) string {
	var short_to_long map[string]string
	short_to_long = map[string]string{
		"master":   "/media/mount-points/Backup-ext4-Mate/restic_master",
		"onedrive": "rclone:onedrive:restic_backups",
		"data":     "/media/wplapper/internal-fast/restic_Data",
		"data2":    "/media/wplapper/internal-fast/restic_Data2",
	}

	if long_name, ok := short_to_long[name]; ok {
		return long_name
	} else {
		return name
	}
}

// provide a database derived from a repository name
func map_repo_names_to_databases(repo_name string) string {
	var repo_to_database map[string]string
	repo_to_database = map[string]string{
		"/media/mount-points/Backup-ext4-Mate/restic_master": "/home/wplapper/restic/db/restic-master.db",
		"rclone:onedrive:restic_backups":                     "/home/wplapper/restic/db/restic-onedrive.db",
		"/media/wplapper/internal-fast/restic_Data":          "/home/wplapper/restic/db/XPS-restic-data.db",
		"/media/wplapper/internal-fast/restic_Data2":         "/home/wplapper/restic/db/XPS-restic-data2.db",

		"master":   "/home/wplapper/restic/db/restic-master.db",
		"onedrive": "/home/wplapper/restic/db/restic-onedrive.db",
		"data":     "/home/wplapper/restic/db/XPS-restic-data.db",
		"data2  ":  "/home/wplapper/restic/db/XPS-restic-data2.db",
	}
	if database_name, ok := repo_to_database[repo_name]; ok {
		return database_name
	} else {
		return repo_name
	}
}
