package ai.liquidway.lfmsmoke

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Scaffold
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.ui.Modifier
import androidx.lifecycle.viewmodel.compose.viewModel

class MainActivity : ComponentActivity() {

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()

        // Debug smoke entrypoint. Trigger headless from CLI with:
        //   adb shell am start -n ai.liquidway.lfmsmoke/.MainActivity \
        //     -e smoke_prompt "Say hello in one short sentence."
        val smokePrompt = intent?.getStringExtra("smoke_prompt")

        setContent {
            MaterialTheme {
                val vm: SmokeViewModel = viewModel()
                val state by vm.state.collectAsState()

                // Auto-run once if launched with a smoke prompt extra.
                // LaunchedEffect is called unconditionally (keyed on the
                // prompt) to respect the rules of composition.
                LaunchedEffect(smokePrompt) {
                    if (smokePrompt != null) {
                        vm.runSmoke(smokePrompt)
                    }
                }

                Scaffold(modifier = Modifier.fillMaxSize()) { inner ->
                    SmokeScreen(
                        state = state,
                        onRun = vm::runSmoke,
                        modifier = Modifier.padding(inner),
                    )
                }
            }
        }
    }
}
