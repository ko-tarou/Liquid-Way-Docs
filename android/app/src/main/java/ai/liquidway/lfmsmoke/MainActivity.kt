package ai.liquidway.lfmsmoke

import ai.liquidway.lfmsmoke.net.LiqMeshService
import ai.liquidway.lfmsmoke.ui.LiqMeshNavHost
import android.Manifest
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.material3.MaterialTheme
import androidx.compose.ui.Modifier
import androidx.core.content.ContextCompat

class MainActivity : ComponentActivity() {

    // POST_NOTIFICATIONS (Android 13+) gates the foreground-service
    // notification. We start the mesh regardless of the answer — the service
    // still runs; only the notification visibility depends on the grant.
    private val notifPermission = registerForActivityResult(
        ActivityResultContracts.RequestPermission(),
    ) { /* result irrelevant to mesh operation */ }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()

        requestNotificationPermissionIfNeeded()
        LiqMeshService.start(this)

        setContent {
            MaterialTheme {
                LiqMeshNavHost(modifier = Modifier.fillMaxSize())
            }
        }
    }

    private fun requestNotificationPermissionIfNeeded() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) return
        val granted = ContextCompat.checkSelfPermission(
            this,
            Manifest.permission.POST_NOTIFICATIONS,
        ) == PackageManager.PERMISSION_GRANTED
        if (!granted) {
            notifPermission.launch(Manifest.permission.POST_NOTIFICATIONS)
        }
    }
}
