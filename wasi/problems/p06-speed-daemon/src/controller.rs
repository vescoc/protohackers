use std::collections::{HashMap, HashSet};

use tracing::info;

const UNIX_DAY: u32 = 86400;

#[derive(Debug, PartialEq)]
#[allow(clippy::struct_field_names)]
pub struct Plate {
    pub(crate) road: u16,
    pub(crate) mile: u16,
    pub(crate) limit: u16,
    pub(crate) plate: String,
    pub(crate) timestamp: u32,
}

#[derive(Debug, PartialEq)]
pub struct Ticket {
    pub(crate) plate: String,
    pub(crate) road: u16,
    pub(crate) mile1: u16,
    pub(crate) timestamp1: u32,
    pub(crate) mile2: u16,
    pub(crate) timestamp2: u32,
    pub(crate) speed: u16,
}

#[derive(Default)]
pub(crate) struct Controller {
    observations: HashMap<(String, u16), HashSet<(u16, u32)>>,
    tickets: HashSet<(String, u32)>,
}

impl Controller {
    #[allow(clippy::cast_possible_truncation)]
    #[tracing::instrument(skip(self))]
    pub(crate) fn signal(
        &mut self,
        Plate {
            road,
            mile,
            limit,
            plate,
            timestamp,
        }: Plate,
    ) -> Vec<Ticket> {
        let limit = limit * 100;

        let car_observations = self.observations.entry((plate.clone(), road)).or_default();

        let mut tickets = vec![];
        if !car_observations.contains(&(mile, timestamp)) {
            for (old_mile, old_timestamp) in car_observations.iter().copied() {
                let delta_timestamp =
                    (i64::from(timestamp) - i64::from(old_timestamp)).unsigned_abs() as u32;
                let speed = if delta_timestamp > 0 {
                    let delta_miles =
                        (i32::from(mile) - i32::from(old_mile)).unsigned_abs() * 100 * 3600;
                    let speed = delta_miles.saturating_div(delta_timestamp);
                    if speed > u32::from(u16::MAX) {
                        u16::MAX
                    } else {
                        speed as u16
                    }
                } else {
                    u16::MAX
                };

                if speed > limit && (speed - limit) >= 50 {
                    let (timestamp1, mile1) = if timestamp > old_timestamp {
                        (old_timestamp, old_mile)
                    } else {
                        (timestamp, mile)
                    };

                    let (timestamp2, mile2) = if timestamp > old_timestamp {
                        (timestamp, mile)
                    } else {
                        (old_timestamp, old_mile)
                    };

                    let day1 = timestamp1 / UNIX_DAY;
                    let day2 = timestamp2 / UNIX_DAY;

                    let insert =
                        (day1..=day2).all(|day| !self.tickets.contains(&(plate.clone(), day)));
                    if insert {
                        info!("ticket insert for {day1}..={day2}");

                        for day in day1..=day2 {
                            let key = (plate.clone(), day);
                            self.tickets.insert(key);
                        }

                        tickets.push(Ticket {
                            plate: plate.clone(),
                            road,
                            mile1,
                            timestamp1,
                            mile2,
                            timestamp2,
                            speed,
                        });
                    } else {
                        info!("ticket already inserted {day1}..={day2}");
                    }
                }
            }

            car_observations.insert((mile, timestamp));
        }

        tickets
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session() {
        let mut controller = Controller::default();

        let tickets = controller.signal(Plate {
            road: 123,
            mile: 8,
            limit: 60,
            plate: "UN1X".to_string(),
            timestamp: 0,
        });
        assert_eq!(tickets, []);

        let mut tickets = controller.signal(Plate {
            road: 123,
            mile: 9,
            limit: 60,
            plate: "UN1X".to_string(),
            timestamp: 45,
        });
        assert_eq!(tickets.len(), 1);
        assert_eq!(
            tickets.pop().unwrap(),
            Ticket {
                plate: "UN1X".to_string(),
                road: 123,
                mile1: 8,
                timestamp1: 0,
                mile2: 9,
                timestamp2: 45,
                speed: 8000,
            }
        );
    }

    #[test]
    fn test_max_one_tickets_a_day() {
        let mut controller = Controller::default();

        let _ = controller.signal(Plate {
            road: 123,
            mile: 8,
            limit: 60,
            plate: "UN1X".to_string(),
            timestamp: 0,
        });

        let _ = controller.signal(Plate {
            road: 123,
            mile: 9,
            limit: 60,
            plate: "UN1X".to_string(),
            timestamp: 45,
        });

        let _ = controller.signal(Plate {
            road: 321,
            mile: 8,
            limit: 60,
            plate: "UN1X".to_string(),
            timestamp: 0,
        });

        let tickets = controller.signal(Plate {
            road: 321,
            mile: 9,
            limit: 60,
            plate: "UN1X".to_string(),
            timestamp: 45,
        });
        assert_eq!(tickets, []);

        let _ = controller.signal(Plate {
            road: 321,
            mile: 8,
            limit: 60,
            plate: "UN1X".to_string(),
            timestamp: UNIX_DAY,
        });

        let mut tickets = controller.signal(Plate {
            road: 321,
            mile: 9,
            limit: 60,
            plate: "UN1X".to_string(),
            timestamp: 45 + UNIX_DAY,
        });
        assert_eq!(tickets.len(), 1);
        assert_eq!(
            tickets.pop().unwrap(),
            Ticket {
                plate: "UN1X".to_string(),
                road: 321,
                mile1: 8,
                timestamp1: UNIX_DAY,
                mile2: 9,
                timestamp2: 45 + UNIX_DAY,
                speed: 8000,
            }
        );
    }
}
