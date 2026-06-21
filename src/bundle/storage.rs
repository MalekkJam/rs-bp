use std::fs::File;
use std::io::Write;

const MAXIMUM_CAPACITY: usize = 1_000;
const FILE_PATH: &str = "storage/bundles.json";

pub struct Storage {
    capacity: usize, // max number of bundles to store
    bundles: Vec<StoredBundle>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            capacity: MAXIMUM_CAPACITY,
        }
    }

    fn get_capacity(&self) -> usize {
        self.capacity
    }

    fn store_bundle(&mut self, bundle: Bundle) -> Result {
        if (self.capacity == 0) {
            return Err("Storage is full");
        } else {
            let stored = StoredBundle {
                bundle,
                status: BundleStatus::Pending,
            };
            save_bundle_to_storage(stored);
            self.capacity -= 1;
            return Ok(());
        }
    }

    fn delete_bundle(&self, bundle_id: Uuid) -> Result {
        if (self.capacity == MAXIMUM_CAPACITY) {
            return Err("Storage is empty");
        } else {
            delete_bundle_from_storage(bundle_id);
            self.capacity += 1;
            return Ok(());
        }
    }

    fn save_bundle_to_storage(stored: StoredBundle) {
        let data = serde_json::to_string(&stored).unwrap();
        let mut file = File::create(FILE_PATH).unwrap();
        file.write_all(data.as_bytes()).unwrap();
    }

    fn delete_bundle_from_storage(bundle_id: Uuid) {
        let mut bundles = load_bundles_from_storage();
        bundles.retain(|b| b.id != bundle_id);
        save_bundles_to_storage(bundles);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn storage_with_capacity(cap: usize) -> Storage {
        Storage { capacity: cap }
    }

    #[test]
    fn test_new_initializes_with_max_capacity() {
        let storage = Storage::new();
        assert_eq!(storage.get_capacity(), MAXIMUM_CAPACITY);
    }

    #[test]
    fn test_get_capacity_returns_current_capacity() {
        let storage = storage_with_capacity(42);
        assert_eq!(storage.get_capacity(), 42);
    }

    #[test]
    fn test_store_bundle_returns_err_when_capacity_is_zero() {
        let mut storage = storage_with_capacity(0);
        // Bundle construction is skipped here because the error path is
        // triggered before any bundle data is used.
        let dummy_bundle = make_dummy_bundle();
        let result = storage.store_bundle(dummy_bundle);
        assert!(result.is_err(), "expected Err when storage is full");
    }

    #[test]
    fn test_store_bundle_decrements_capacity_on_success() {
        let mut storage = storage_with_capacity(5);
        let bundle = make_dummy_bundle();
        // This will also exercise the file-write path; ensure the storage/
        // directory exists before running (or mock FILE_PATH in integration tests).
        let _ = storage.store_bundle(bundle);
        assert_eq!(storage.get_capacity(), 4);
    }

    #[test]
    fn test_delete_bundle_returns_err_when_storage_is_empty() {
        let mut storage = Storage::new();
        let result = storage.delete_bundle(uuid::Uuid::new_v4());
        assert!(result.is_err(), "expected Err when storage is empty");
    }

    #[test]
    fn test_delete_bundle_increments_capacity_on_success() {
        let mut storage = storage_with_capacity(MAXIMUM_CAPACITY - 1);
        let _ = storage.delete_bundle(uuid::Uuid::new_v4());
        assert_eq!(storage.get_capacity(), MAXIMUM_CAPACITY);
    }

    #[test]
    fn test_store_then_delete_restores_capacity() {
        let mut storage = storage_with_capacity(1);
        let bundle = make_dummy_bundle();
        let id = bundle.id;

        let store_result = storage.store_bundle(bundle);
        assert!(store_result.is_ok());
        assert_eq!(storage.get_capacity(), 0);

        let delete_result = storage.delete_bundle(id);
        assert!(delete_result.is_ok());
        assert_eq!(storage.get_capacity(), 1);
    }

    fn make_dummy_bundle() -> Bundle {
        use chrono::Utc;
        Bundle {
            id: uuid::Uuid::new_v4(),
            source: Default::default(),
            destination: Default::default(),
            content: String::from("test payload"),
            timestamp: Utc::now(),
            ttl: Utc::now() + chrono::Duration::seconds(3600),
            kind: BundleKind::Message,
            shipment_status: MsgStatus::Pending,
        }
    }
}
