package ai.liquidway.lfmsmoke.ui

import ai.liquidway.lfmsmoke.settings.SettingsRepository
import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch

class SettingsViewModel(app: Application) : AndroidViewModel(app) {

    private val settings = SettingsRepository.get(app)

    init {
        // Seed a stable default device name on first run.
        viewModelScope.launch { settings.ensureDefaultsInitialized() }
    }

    val serverMode: StateFlow<Boolean> = settings.serverMode
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), false)

    val deviceName: StateFlow<String> = settings.deviceName
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), "")

    val serverHost: StateFlow<String> = settings.serverHost
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), "")

    val bridgeEnabled: StateFlow<Boolean> = settings.bridgeEnabled
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), false)

    val bridgeHost: StateFlow<String> = settings.bridgeHost
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), "")

    val deviceId: StateFlow<String> = kotlinx.coroutines.flow.flow {
        emit(settings.deviceId())
    }.stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), "")

    fun setServerMode(enabled: Boolean) {
        viewModelScope.launch { settings.setServerMode(enabled) }
    }

    fun setDeviceName(name: String) {
        viewModelScope.launch { settings.setDeviceName(name) }
    }

    fun setServerHost(host: String) {
        viewModelScope.launch { settings.setServerHost(host) }
    }

    fun setBridgeEnabled(enabled: Boolean) {
        viewModelScope.launch { settings.setBridgeEnabled(enabled) }
    }

    fun setBridgeHost(host: String) {
        viewModelScope.launch { settings.setBridgeHost(host) }
    }
}
