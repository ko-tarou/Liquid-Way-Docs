package ai.liquidway.lfmsmoke.data

import android.content.Context
import androidx.room.Database
import androidx.room.Room
import androidx.room.RoomDatabase
import androidx.room.TypeConverter
import androidx.room.TypeConverters

/** Stores [MessageStatus] as its name; resilient to enum ordinal changes. */
class Converters {
    @TypeConverter
    fun fromStatus(status: MessageStatus): String = status.name

    @TypeConverter
    fun toStatus(value: String): MessageStatus = MessageStatus.valueOf(value)
}

@Database(
    entities = [Message::class],
    version = 1,
    exportSchema = false,
)
@TypeConverters(Converters::class)
abstract class AppDatabase : RoomDatabase() {

    abstract fun messageDao(): MessageDao

    companion object {
        @Volatile
        private var instance: AppDatabase? = null

        /**
         * Process-wide singleton. Double-checked locking keeps construction
         * cheap and thread-safe without pulling in a DI framework (Hilt would
         * be overkill for a single database at this stage).
         */
        fun get(context: Context): AppDatabase {
            return instance ?: synchronized(this) {
                instance ?: Room.databaseBuilder(
                    context.applicationContext,
                    AppDatabase::class.java,
                    "liqmesh.db",
                ).build().also { instance = it }
            }
        }
    }
}
