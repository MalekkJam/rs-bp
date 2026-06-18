
const MAXIMUM_CAPACITY: usize = 1_000; 

pub struct Storage {
    capacity : usize, // max number of bundles to store
}


impl Storage {
    pub fn new() -> Self {
        Storage { capacity: MAXIMUM_CAPACITY }
    }

    fn get_capacity(&self) -> usize {
        self.capacity
    }

    pub fn store_bundle(&self, bundle : Bundle) -> Result {
        if (self.capacity == 0) {
            return Err("Storage is full");
        }
        else {
            save_bundle_to_storage(bundle);
            self.capacity -= 1;
            return Ok(());
        }
    }

    fn save_bundle_to_storage(bundle : Bundle) {
        
    }

}   

