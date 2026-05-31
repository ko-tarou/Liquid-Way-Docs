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
        val SERVER_HOST = stringPreferencesKey("server_host")
        val BRIDGE_ENABLED = booleanPreferencesKey("bridge_enabled")
        val BRIDGE_HOST = stringPreferencesKey("bridge_host")
    }

    val serverMode: Flow<Boolean> =
        dataStore.data.map { it[Keys.SERVER_MODE] ?: false }

    /**
     * Hub IP/hostname a leaf connects to (layer 2). Manual entry is the MVP:
     * it is deterministic and avoids the flakiness of NSD/mDNS discovery on
     * locked-down or multi-subnet Wi-Fi. Empty until the user enters one.
     */
    val serverHost: Flow<String> =
        dataStore.data.map { it[Keys.SERVER_HOST] ?: "" }

    /**
     * Stage-1 bridge: whether THIS hub also links to a *second* hub so the two
     * star networks merge into one chat. Default **false** — when off, the
     * networking layer never creates a bridge transport, so behaviour is
     * byte-for-byte identical to the pre-bridge star (zero regression).
     *
     * Only meaningful when [serverMode] is on (a leaf has nothing to bridge);
     * the UI hides it otherwise and [MeshController] gates on serverMode too.
     */
    val bridgeEnabled: Flow<Boolean> =
        dataStore.data.map { it[Keys.BRIDGE_ENABLED] ?: false }

    /**
     * The peer hub's LAN IP this hub bridges to. Only ONE of the two hubs sets
     * this (the other leaves it blank) so there is exactly one inter-hub TCP
     * link — setting it on both would create a duplicate link. Empty until set.
     */
    val bridgeHost: Flow<String> =
        dataStore.data.map { it[Keys.BRIDGE_HOST] ?: "" }

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

    suspend fun setServerHost(host: String) {
        dataStore.edit { it[Keys.SERVER_HOST] = host.trim() }
    }

    suspend fun setBridgeEnabled(enabled: Boolean) {
        dataStore.edit { it[Keys.BRIDGE_ENABLED] = enabled }
    }

    suspend fun setBridgeHost(host: String) {
        dataStore.edit { it[Keys.BRIDGE_HOST] = host.trim() }
    }

    /** Current resolved hub host (empty string if unset). */
    suspend fun currentServerHost(): String =
        dataStore.data.first()[Keys.SERVER_HOST] ?: ""

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
