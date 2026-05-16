package ai.liquidway.lfmsmoke.ui

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.ArrowBack
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Switch
import androidx.compose.material3.Text
import androidx.compose.material3.TopAppBar
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.viewmodel.compose.viewModel

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun SettingsScreen(
    onBack: () -> Unit,
    modifier: Modifier = Modifier,
    viewModel: SettingsViewModel = viewModel(),
) {
    val serverMode by viewModel.serverMode.collectAsStateWithLifecycle()
    val deviceName by viewModel.deviceName.collectAsStateWithLifecycle()
    val deviceId by viewModel.deviceId.collectAsStateWithLifecycle()
    val serverHost by viewModel.serverHost.collectAsStateWithLifecycle()

    // Local editable copy; seeded from the persisted name once it resolves.
    var nameDraft by remember { mutableStateOf("") }
    LaunchedEffect(deviceName) {
        if (deviceName.isNotEmpty() && nameDraft.isEmpty()) {
            nameDraft = deviceName
        }
    }

    var hostDraft by remember { mutableStateOf("") }
    LaunchedEffect(serverHost) {
        if (serverHost.isNotEmpty() && hostDraft.isEmpty()) {
            hostDraft = serverHost
        }
    }

    Scaffold(
        modifier = modifier,
        topBar = {
            TopAppBar(
                title = { Text("Settings") },
                navigationIcon = {
                    IconButton(onClick = onBack) {
                        Icon(
                            Icons.AutoMirrored.Filled.ArrowBack,
                            contentDescription = "Back",
                        )
                    }
                },
            )
        },
    ) { inner ->
        Column(
            modifier = Modifier
                .fillMaxSize()
                .padding(inner)
                .padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(16.dp),
        ) {
            Row(
                modifier = Modifier.fillMaxWidth(),
                verticalAlignment = Alignment.CenterVertically,
            ) {
                Column(modifier = Modifier.weight(1f)) {
                    Text("Server mode", style = MaterialTheme.typography.titleMedium)
                    Text(
                        "Act as the hub for same-network devices.",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                }
                Switch(
                    checked = serverMode,
                    onCheckedChange = viewModel::setServerMode,
                )
            }

            Text(
                text = if (serverMode) {
                    "Current mode: Hub (server) · listening on port 8765"
                } else {
                    "Current mode: Client (leaf) · connects to the hub below"
                },
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.primary,
            )

            HorizontalDivider()

            // Hub IP only matters for a leaf; hide it in server mode.
            if (!serverMode) {
                Text("Server IP", style = MaterialTheme.typography.titleMedium)
                Text(
                    "The hub device's LAN IP (e.g. 192.168.1.20). Manual entry; " +
                        "find it in the hub's Wi-Fi settings.",
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
                OutlinedTextField(
                    value = hostDraft,
                    onValueChange = { hostDraft = it },
                    modifier = Modifier.fillMaxWidth(),
                    singleLine = true,
                    label = { Text("Hub IP / hostname") },
                    trailingIcon = {
                        androidx.compose.material3.TextButton(
                            onClick = { viewModel.setServerHost(hostDraft) },
                            enabled = hostDraft.isNotBlank() && hostDraft != serverHost,
                        ) {
                            Text("Save")
                        }
                    },
                )

                HorizontalDivider()
            }

            Text("Device name", style = MaterialTheme.typography.titleMedium)
            OutlinedTextField(
                value = nameDraft,
                onValueChange = { nameDraft = it },
                modifier = Modifier.fillMaxWidth(),
                singleLine = true,
                label = { Text("Display name") },
                trailingIcon = {
                    androidx.compose.material3.TextButton(
                        onClick = { viewModel.setDeviceName(nameDraft) },
                        enabled = nameDraft.isNotBlank() && nameDraft != deviceName,
                    ) {
                        Text("Save")
                    }
                },
            )

            HorizontalDivider()

            Text("Device ID", style = MaterialTheme.typography.titleMedium)
            Text(
                text = deviceId.ifEmpty { "…" },
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        }
    }
}
