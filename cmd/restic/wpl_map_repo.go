package main

/* base mapping
 * repo shortname to repo fullname
 */

// map short repo names to full repository names
func map_repo_names(name string) string {
	var short_to_long map[string]string
	short_to_long = map[string]string{
		"master":   "/media/mount-points/Backup-ext4-Mate/restic_master",
		"onedrive": "rclone:onedrive:restic_backups",
		//"onedrive": "rclone:onedrive:restic_backups",
		//"data":     "/media/wplapper/internal-fast/restic_Data",
		//"data2":    "/media/wplapper/internal-fast/restic_Data2",
		"data":           "/media/mount-points/Backup-ext4-Mate/restic_data",
		"home":           "/media/mount-points/Backup-ext4-Mate/restic_home",
		"massive":        "/media/mount-points/Backup-ext4-Mate/restic_massive",
		"rasp_winxp":     "/media/mount-points/Backup-ext4-Mate/restic_rasp_winxp",

		"one_data":       "rclone:onedrive:restic_data",
		"one_home":       "rclone:onedrive:restic_home",
		"one_massive":    "rclone:onedrive:restic_massive",
		"one_rasp_winxp": "rclone:onedrive:restic_rasp_winxp",
	}

	if long_name, ok := short_to_long[name]; ok {
		return long_name
	} else {
		return name
	}
}
