package ai.liquidway.lfmsmoke.net

import ai.liquidway.lfmsmoke.MainActivity
import ai.liquidway.lfmsmoke.ai.LeapSummarizationEngine
import ai.liquidway.lfmsmoke.settings.SettingsRepository
import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.Build
import android.os.IBinder
import android.util.Log
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

/**
 * Keeps the layer-2 socket(s) alive while the app is backgrounded.
 *
 * The service watches (serverMode, serverHost) and asks [MeshController] to
 * reconfigure whenever either changes — so flipping the Settings toggle
 * re-homes the mesh without an app restart. The connection-state notification
 * doubles as the required foreground notification.
 */
class LiqMeshService : Service() {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private lateinit var controller: MeshController
    private lateinit var settings: SettingsRepository
    private var stateJob: Job? = null

    override fun onCreate() {
        super.onCreate()
        controller = MeshController.get(this)
        settings = SettingsRepository.get(this)
        // Layer 4: give the controller a way to build the (heavyweight) LEAP
        // engine lazily. It is only invoked when this device is the hub AND a
        // summary_req actually arrives, so the model is not loaded otherwise.
        if (controller.summarizationEngineProvider == null) {
            val app = applicationContext
            controller.summarizationEngineProvider = {
                LeapSummarizationEngine(app)
            }
        }
        createChannel()
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
            startForeground(
                NOTIF_ID,
                buildNotification("Starting…"),
                ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC,
            )
        } else {
            startForeground(NOTIF_ID, buildNotification("Starting…"))
        }

        // Reconfigure the transport on any (mode, host, bridge) change. The
        // bridge fields are folded in so toggling the bridge (or editing its
        // host) re-homes the mesh live, exactly like the server-mode/host flow.
        combine(
            settings.serverMode,
            settings.serverHost,
            settings.bridgeEnabled,
            settings.bridgeHost,
        ) { mode, host, bridgeEnabled, bridgeHost ->
            MeshConfig(mode, host, bridgeEnabled, bridgeHost)
        }
            .distinctUntilChanged()
            .onEach { cfg ->
                Log.i(
                    TAG,
                    "Settings -> serverMode=${cfg.serverMode} host='${cfg.host}' " +
                        "bridgeEnabled=${cfg.bridgeEnabled} bridgeHost='${cfg.bridgeHost}'",
                )
                controller.configure(cfg.serverMode, cfg.host, cfg.bridgeEnabled, cfg.bridgeHost)
            }
            .launchIn(scope)

        // Mirror connection state into the notification.
        stateJob = scope.launch {
            controller.state.collectLatest { updateNotification(it.describe()) }
        }
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        // Sticky: the OS restarts the service (and thus the mesh) after kill.
        return START_STICKY
    }

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onDestroy() {
        stateJob?.cancel()
        runBlocking { controller.shutdown() }
        scope.coroutineContext.cancel()
        super.onDestroy()
    }

    private fun createChannel() {
        val nm = getSystemService(NotificationManager::class.java)
        val channel = NotificationChannel(
            CHANNEL_ID,
            "LiqMesh status",
            NotificationManager.IMPORTANCE_LOW,
        ).apply { description = "Same-network mesh connection status" }
        nm.createNotificationChannel(channel)
    }

    private fun buildNotification(text: String): Notification {
        val tap = PendingIntent.getActivity(
            this,
            0,
            Intent(this, MainActivity::class.java),
            PendingIntent.FLAG_IMMUTABLE,
        )
        return Notification.Builder(this, CHANNEL_ID)
            .setContentTitle("LiqMesh")
            .setContentText(text)
            .setSmallIcon(android.R.drawable.stat_sys_data_bluetooth)
            .setOngoing(true)
            .setContentIntent(tap)
            .build()
    }

    private fun updateNotification(text: String) {
        getSystemService(NotificationManager::class.java)
            .notify(NOTIF_ID, buildNotification(text))
    }

    companion object {
        private const val TAG = "LiqMesh/Svc"
        private const val CHANNEL_ID = "liqmesh_status"
        private const val NOTIF_ID = 0x10A9

        /** Starts the service as a foreground data-sync service. */
        fun start(context: Context) {
            val intent = Intent(context, LiqMeshService::class.java)
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                context.startForegroundService(intent)
            } else {
                context.startService(intent)
            }
        }

        fun stop(context: Context) {
            context.stopService(Intent(context, LiqMeshService::class.java))
        }
    }
}

/**
 * The full transport configuration the service watches. A value class so
 * [distinctUntilChanged] suppresses re-configure unless something actually
 * changed (cheap structural equality across the four settings).
 */
private data class MeshConfig(
    val serverMode: Boolean,
    val host: String,
    val bridgeEnabled: Boolean,
    val bridgeHost: String,
)

/** Human-readable one-liner for the notification / future UI reuse. */
private fun MeshState.describe(): String = when (this) {
    is MeshState.Idle -> "Idle"
    is MeshState.Hub -> "Hub · $peerCount peer(s)"
    is MeshState.Connecting -> "Connecting to $host…"
    is MeshState.Connected -> "Connected to $host"
    is MeshState.Disconnected -> "Disconnected · $reason"
}
