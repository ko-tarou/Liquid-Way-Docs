package ai.liquidway.lfmsmoke.settings

import android.content.Context
import androidx.datastore.core.DataStore
import androidx.datastore.preferences.core.Preferences
import androidx.datastore.preferences.core.booleanPreferencesKey
import androidx.datastore.preferences.core.edit
import androidx.datastore.preferences.core.stringPreferencesKey
import androidx.datastore.preferences.preferencesDataStore
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import java.util.UUID

// Single DataStore instance per process, scoped to the application context.
private val Context.settingsDataStore: DataStore<Preferences> by preferencesDataStore(
    name = "liqmesh_settings",
)

/**
 * Persists user/device settings:
 *  - [serverMode]  : whether this device acts as the star-topology hub
 *                    (consumed by the future networking layer; default false).
 *  - [deviceId]    : stable per-install identity, generated once and reused.
 *  - [deviceName]  : human-readable name, defaulted then editable.
 */
class SettingsRepository(context: Context) {

    private val dataStore = context.applicationContext.settingsDataStore

    private object Keys {
        val SERVER_MODE = booleanPreferencesKey("server_mode")
        val DEVICE_ID = stringPreferencesKey("device_id")
        val DEVICE_NAME = stringPreferencesKey("device_name")
    }

    val serverMode: Flow<Boolean> =
        dataStore.data.map { it[Keys.SERVER_MODE] ?: false }

    /**
     * Emits the persisted device name. On first run no name is stored yet, so
     * [ensureDefaultsInitialized] seeds a stable random default; until that
     * completes a fixed placeholder is emitted (never a flickering random one).
     */
    val deviceName: Flow<String> =
        dataStore.data.map { it[Keys.DEVICE_NAME] ?: PENDING_NAME }

    suspend fun setServerMode(enabled: Boolean) {
        dataStore.edit { it[Keys.SERVER_MODE] = enabled }
    }

    suspend fun setDeviceName(name: String) {
        val trimmed = name.trim()
        if (trimmed.isEmpty()) return
        dataStore.edit { it[Keys.DEVICE_NAME] = trimmed }
    }

    /**
     * Returns the stable device id, generating and persisting one on first
     * access. Subsequent calls return the same value for the install lifetime.
     */
    suspend fun deviceId(): String {
        val existing = dataStore.data.first()[Keys.DEVICE_ID]
        if (existing != null) return existing
        val generated = UUID.randomUUID().toString()
        dataStore.edit { prefs ->
            // Re-check under edit() to avoid racing two first-time callers.
            if (prefs[Keys.DEVICE_ID] == null) prefs[Keys.DEVICE_ID] = generated
        }
        return dataStore.data.first()[Keys.DEVICE_ID] ?: generated
    }

    /** Convenience accessor for the current (resolved) device name. */
    suspend fun currentDeviceName(): String {
        ensureDefaultsInitialized()
        return dataStore.data.first()[Keys.DEVICE_NAME] ?: defaultDeviceName()
    }

    /**
     * Seeds a stable random default device name on first run so the UI never
     * shows a flickering or placeholder name. Idempotent and race-safe.
     */
    suspend fun ensureDefaultsInitialized() {
        if (dataStore.data.first()[Keys.DEVICE_NAME] != null) return
        val generated = defaultDeviceName()
        dataStore.edit { prefs ->
            if (prefs[Keys.DEVICE_NAME] == null) prefs[Keys.DEVICE_NAME] = generated
        }
    }

    private fun defaultDeviceName(): String {
        val suffix = (1000..9999).random()
        return "device-$suffix"
    }

    companion object {
        private const val PENDING_NAME = "device-…"

        @Volatile
        private var instance: SettingsRepository? = null

        fun get(context: Context): SettingsRepository {
            return instance ?: synchronized(this) {
                instance ?: SettingsRepository(context).also { instance = it }
            }
        }
    }
}
