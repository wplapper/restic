package main

/* wpl 2023-08-01 and 2023-12-30
 * base mapping
 * repo shortname to repo fullname
 */

// map short repo names to full repository names
func map_repo_names(name string) string {
	shortToLong := map[string]string{
		"data":        "/media/mount-points/Backup-ext4-Mate/restic_data",
		"home":        "/media/mount-points/Backup-ext4-Mate/restic_home",
		"massive":     "/media/mount-points/Backup-ext4-Mate/restic_massive",
		"rasp":        "/media/mount-points/Backup-ext4-Mate/restic_rasp_winxp",

		"one_data":    "rclone:onedrive:restic_data",
		"one_home":    "rclone:onedrive:restic_home",
		"one_massive": "rclone:onedrive:restic_massive",
		"one_rasp":    "rclone:onedrive:restic_rasp_winxp",
	}

	if longName, ok := shortToLong[name]; ok {
		return longName
	} else {
		return name
	}
}
