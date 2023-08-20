package main

/* base mapping
 * repo shortname to repo fullname
 */

// map short repo names to full repository names
func map_repo_names(name string) string {
	var short_to_long map[string]string
	short_to_long = map[string]string{
		"data":           "/media/mount-points/Backup-ext4-Mate/restic_data",
		"home":           "/media/mount-points/Backup-ext4-Mate/restic_home",
		"massive":        "/media/mount-points/Backup-ext4-Mate/restic_massive",
		"mass":           "/media/mount-points/Backup-ext4-Mate/restic_massive",
		"rasp_winxp":     "/media/mount-points/Backup-ext4-Mate/restic_rasp_winxp",
		"rasp":           "/media/mount-points/Backup-ext4-Mate/restic_rasp_winxp",

		"one_data":       "rclone:onedrive:restic_data",
		"one_home":       "rclone:onedrive:restic_home",
		"one_massive":    "rclone:onedrive:restic_massive",
		"one_mass":       "rclone:onedrive:restic_massive",
		"one_rasp_winxp": "rclone:onedrive:restic_rasp_winxp",
		"one_rasp":       "rclone:onedrive:restic_rasp_winxp",
		"onedrive":       "rclone:onedrive:restic_backups",
	}

	if long_name, ok := short_to_long[name]; ok {
		return long_name
	} else {
		return name
	}
}
