from __future__ import annotations

import npyscreen

class VPSConfigForm(npyscreen.FormBaseNewWithMenus):  # Using FormBaseNewWithMenus for potential future menu additions
    def create(self):
        self.name = "VPS Configuration"

        # Get existing config values (will be properly loaded later)
        # For now, using placeholders or direct default values from the directive
        # self.config = self.parentApp.manager.config # This will be the way to get it

        self.vps_enabled = self.add(npyscreen.Checkbox, name="VPS Enabled", value=False) # Placeholder
        self.vps_host = self.add(npyscreen.TitleText, name="VPS Host:", value="") # Placeholder
        self.vps_port = self.add(npyscreen.TitleText, name="Port:", value="22")
        self.vps_username = self.add(npyscreen.TitleText, name="Username:", value="") # Placeholder
        self.vps_key_path = self.add(npyscreen.TitleFilename, name="SSH Key Path:", value="~/.ssh/id_rsa")
        self.vps_remote_base_path = self.add(npyscreen.TitleText, name="Remote Base Path:", value="/data/spectra")

        self.add(npyscreen.FixedText, value="-"*40, editable=False, rely=self.nextrely()+1)
        self.add(npyscreen.FixedText, value="Directory Structure (Templates):", editable=False)
        self.dir_archives = self.add(npyscreen.TitleText, name="Archives:", value="archives/{date}/{channel_name}")
        self.dir_media = self.add(npyscreen.TitleText, name="Media:", value="media/{type}/{date}")
        self.dir_text_files = self.add(npyscreen.TitleText, name="Text Files:", value="documents/text/{channel_name}")
        self.dir_cloud_downloads = self.add(npyscreen.TitleText, name="Cloud Downloads:", value="cloud/{date}/{channel_name}")

        self.add(npyscreen.FixedText, value="-"*40, editable=False, rely=self.nextrely()+1)
        self.add(npyscreen.FixedText, value="Sync Options:", editable=False)
        self.sync_auto = self.add(npyscreen.Checkbox, name="Auto Sync Enabled", value=False) # Placeholder
        self.sync_interval = self.add(npyscreen.TitleSlider, name="Sync Interval (min):", out_of=120, step=5, value=30) # Max 2 hours
        self.sync_compression = self.add(npyscreen.Checkbox, name="Compression (rsync)", value=True)
        self.sync_delete_after = self.add(npyscreen.Checkbox, name="Delete Local After Sync", value=False)

        # Placeholder for status messages and buttons
        self.status_widget = self.add(npyscreen.FixedText, name="Status:", value="", editable=False, rely=self.nextrely()+2)

        # Menu for actions like Save, Test Connection, Back
        m = self.new_menu(name="Actions")
        m.addItem("Save Configuration", self.save_config, shortcut='^S')
        m.addItem("Test Connection", self.test_connection, shortcut='^T')
        m.addItem("Back to Settings", self.switch_to_settings, shortcut='^B') # Or Main Menu

    def beforeEditing(self):
        """Load current config values into the form fields before display."""
        if hasattr(self.parentApp, 'manager') and self.parentApp.manager and hasattr(self.parentApp.manager, 'config'):
            cfg = self.parentApp.manager.config
            vps_conf = cfg.vps_conf # Use the property we created

            self.vps_enabled.value = vps_conf.get("enabled", False)
            self.vps_host.value = vps_conf.get("host", "")
            self.vps_port.value = str(vps_conf.get("port", 22))
            self.vps_username.value = vps_conf.get("username", "")
            self.vps_key_path.value = vps_conf.get("key_path", "~/.ssh/id_rsa")
            self.vps_remote_base_path.value = vps_conf.get("remote_base_path", "/data/spectra")

            dir_struct = vps_conf.get("directory_structure", {})
            self.dir_archives.value = dir_struct.get("archives", "archives/{date}/{channel_name}")
            self.dir_media.value = dir_struct.get("media", "media/{type}/{date}")
            self.dir_text_files.value = dir_struct.get("text_files", "documents/text/{channel_name}")
            self.dir_cloud_downloads.value = dir_struct.get("cloud_downloads", "cloud/{date}/{channel_name}")

            sync_opts = vps_conf.get("sync_options", {})
            self.sync_auto.value = sync_opts.get("auto_sync", False)
            self.sync_interval.value = sync_opts.get("sync_interval_minutes", 30)
            self.sync_compression.value = sync_opts.get("compression", True)
            self.sync_delete_after.value = sync_opts.get("delete_after_sync", False)
        else:
            self.status_widget.value = "Error: Config manager not found."
            self.status_widget.display()


    def save_config(self):
        """Save the current form values to the configuration file."""
        if not (hasattr(self.parentApp, 'manager') and self.parentApp.manager and hasattr(self.parentApp.manager, 'config')):
            npyscreen.notify_confirm("Error: Config manager not found. Cannot save.", title="Save Error")
            return

        cfg = self.parentApp.manager.config

        # Basic validation example (can be expanded)
        try:
            port = int(self.vps_port.value)
            if not (0 < port < 65536):
                raise ValueError("Port number out of range.")
        except ValueError:
            npyscreen.notify_confirm("Invalid port number. Please enter a value between 1 and 65535.", title="Validation Error")
            return

        cfg.data["vps"]["enabled"] = self.vps_enabled.value
        cfg.data["vps"]["host"] = self.vps_host.value
        cfg.data["vps"]["port"] = int(self.vps_port.value) # Ensure it's an int
        cfg.data["vps"]["username"] = self.vps_username.value
        cfg.data["vps"]["key_path"] = self.vps_key_path.value
        cfg.data["vps"]["remote_base_path"] = self.vps_remote_base_path.value

        cfg.data["vps"]["directory_structure"]["archives"] = self.dir_archives.value
        cfg.data["vps"]["directory_structure"]["media"] = self.dir_media.value
        cfg.data["vps"]["directory_structure"]["text_files"] = self.dir_text_files.value
        cfg.data["vps"]["directory_structure"]["cloud_downloads"] = self.dir_cloud_downloads.value

        cfg.data["vps"]["sync_options"]["auto_sync"] = self.sync_auto.value
        cfg.data["vps"]["sync_options"]["sync_interval_minutes"] = int(self.sync_interval.value) # Ensure int
        cfg.data["vps"]["sync_options"]["compression"] = self.sync_compression.value
        cfg.data["vps"]["sync_options"]["delete_after_sync"] = self.sync_delete_after.value

        try:
            cfg.save()
            npyscreen.notify_confirm("VPS configuration saved successfully!", title="Success")
            self.status_widget.value = "Configuration saved."
        except Exception as e:
            npyscreen.notify_confirm(f"Error saving configuration: {e}", title="Save Error")
            self.status_widget.value = f"Error saving: {e}"
        self.status_widget.display()

    def test_connection(self):
        """Placeholder for testing SSH connection to the VPS."""
        # This will be implemented later, likely involving VPSSyncEngine
        npyscreen.notify_confirm("Connection test functionality not yet implemented.", title="Coming Soon")
        # In a real implementation:
        # 1. Get current form values.
        # 2. Call a method (e.g., in VPSSyncEngine) to test connection.
        # 3. Display success/failure message.
        # self.status_widget.value = "Testing connection..."
        # self.status_widget.display()
        # result = self.parentApp.manager.vps_sync_engine.test_connection(self.vps_host.value, ...)
        # if result:
        #    self.status_widget.value = "Connection successful!"
        # else:
        #    self.status_widget.value = "Connection failed. Check settings and logs."
        # self.status_widget.display()

    def switch_to_settings(self):
        # Assuming there's a "SETTINGS" form or back to "MAIN"
        # This might need adjustment based on actual TUI structure
        self.parentApp.switchFormPrevious() # Or "MAIN" / "SETTINGS"

    def on_cancel(self): # Override default on_cancel if needed
        self.switch_to_settings()
