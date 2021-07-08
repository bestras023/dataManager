using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SQLite;

namespace OpenKey_LMS
{
    public class DataManager
    {
        const string connectionString = "Data Source=database.db; Version=3; FailIfMissing=True; Foreign Keys=True;";

        private static int ExecuteWrite(string query, Dictionary<string, object> args)
        {
            int numberOfRowsAffected;

            //setup the connection to the database
            using (var con = new SQLiteConnection(connectionString))
            {
                con.Open();

                //open a new command
                using (var cmd = new SQLiteCommand(query, con))
                {
                    //set the arguments given in the query
                    foreach (var pair in args)
                    {
                        cmd.Parameters.AddWithValue(pair.Key, pair.Value);
                    }

                    //execute the query and get the number of row affected
                    numberOfRowsAffected = cmd.ExecuteNonQuery();
                }

                return numberOfRowsAffected;
            }
        }

        private static DataTable ExecuteRead(string query, Dictionary<string, object> args)
        {
            if (string.IsNullOrEmpty(query.Trim()))
                return null;

            using (var con = new SQLiteConnection(connectionString))
            {
                con.Open();
                using (var cmd = new SQLiteCommand(query, con))
                {
                    if (args != null)
                    {
                        foreach (var pair in args)
                        {
                            cmd.Parameters.AddWithValue(pair.Key, pair.Value);
                        }
                    }

                    var da = new SQLiteDataAdapter(cmd);
                    var dt = new DataTable();
                    da.Fill(dt);
                    da.Dispose();
                    return dt;
                }
            }
        }

        private static void UpdateModifiedTime(string column, DateTime currTime)
        {
            const string query = "INSERT OR REPLACE INTO modified_time(item, time) VALUES(@item, @time)";
            var args = new Dictionary<string, object>
            {
                {"@item", column},
                {"@time", currTime.ToString("yyyy-MM-dd HH:mm:ss.fff")}
            };
            ExecuteWrite(query, args);
        }

        public static int AddDepartment(Department department, DateTime currTime)
        {
            const string query = "INSERT INTO departments(name, description) VALUES(@name, @description)";

            //here we are setting the parameter values that will be actually 
            //replaced in the query in Execute method
            var args = new Dictionary<string, object>
            {
                {"@name", department.name},
                {"@description", department.description}
            };

            try
            {
                int affected = ExecuteWrite(query, args);
                UpdateModifiedTime("config", currTime);
                return affected;
            }
            catch (SQLiteException ex)
            {
                if (ex.Message.StartsWith("constraint failed"))
                {
                    WpfMessageBox.Show("Department exists!");
                    return 0;
                }
                else
                {
                    WpfMessageBox.Show("Cannot add department!");
                    return 0;
                }
            }
            // Update modified time
        }

        public static int UpdateDepartment(Department department, DateTime currTime)
        {
            const string query = "UPDATE departments SET name=@name, description=@description WHERE id=@id";

            var args = new Dictionary<string, object>
            {
                {"@id", department.id},
                {"@name", department.name},
                {"@description", department.description}
            };

            try
            {
                int affected = ExecuteWrite(query, args);
                UpdateModifiedTime("config", currTime);
                return affected;
            }
            catch
            {
                WpfMessageBox.Show("Cannot edit Department!", MessageBoxType.Error);
                return 0;
            }
        }

        public static int DeleteDepartment(int id, DateTime currTime)
        {
            const string query = "DELETE FROM departments WHERE Id=@id";

            var args = new Dictionary<string, object>
            {
                {"@id", id},
            };

            try
            {
                int affected = ExecuteWrite(query, args);
                UpdateModifiedTime("config", currTime);
                return affected;
            }
            catch (SQLiteException ex)
            {
                if (ex.Message.StartsWith("constraint failed") && ex.Message.Contains("FOREIGN KEY"))
                {
                    WpfMessageBox.Show("Users in the department exist!", MessageBoxType.Error);
                    return 0;
                }
                else
                {
                    WpfMessageBox.Show("Cannot delete department!", MessageBoxType.Error);
                    return 0;
                }
            }
        }

        public static List<Department> GetDepartmentList()
        {
            var query = "SELECT * FROM departments";

            DataTable dt = ExecuteRead(query, null);

            if (dt == null || dt.Rows.Count == 0)
            {
                return null;
            }

            List<Department> departmentList = new List<Department>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                var department = new Department
                {
                    id = Convert.ToInt32(dt.Rows[i]["id"]),
                    name = Convert.ToString(dt.Rows[i]["name"]),
                    description = Convert.ToString(dt.Rows[i]["description"])
                };
                departmentList.Add(department);
            }

            return departmentList;
        }

        public static int GetMaxID(string tablename)
        {
            var query = string.Format("SELECT MAX(id) as max_id FROM {0}", tablename);
            DataTable dt = ExecuteRead(query, null);

            try
            {
                return Convert.ToInt32(dt.Rows[0]["max_id"]);
            }
            catch
            {
                return 0;
            }
        }

        public static int AddUser(User user, DateTime currTime)
        {
            const string query = "INSERT INTO users(id, name, password, access, department) VALUES(@id, @name, @password, @access, @department)";

            var args = new Dictionary<string, object>
            {
                {"@id", user.id},
                {"@name", user.name},
                {"@password", user.password},
                {"@access", user.access},
                {"@department", user.department}
            };

            try
            {
                int affected = ExecuteWrite(query, args);
                UpdateModifiedTime("config", currTime);
                return affected;
            }
            catch (SQLiteException ex)
            {
                if (ex.Message.StartsWith("constraint failed") && ex.Message.Contains("UNIQUE"))
                {
                    WpfMessageBox.Show("User exists!", MessageBoxType.Error);
                    return 0;
                }
                else
                {
                    WpfMessageBox.Show("Cannot add user!", MessageBoxType.Error);
                    return 0;
                }
            }
        }

        public static int UpdateUser(User user, bool changePassword, DateTime currTime)
        {
            const string query1 = "UPDATE users SET name=@name, password=@password, access=@access, department=@department WHERE id=@id";
            const string query2 = "UPDATE users SET name=@name, access=@access, department=@department WHERE id=@id";

            var args = new Dictionary<string, object>
            {
                {"@id", user.id},
                {"@name", user.name},
                {"@password", user.password},
                {"@access", user.access},
                {"@department", user.department}
            };

            try
            {
                if (changePassword)
                {
                    int affected = ExecuteWrite(query1, args);
                    UpdateModifiedTime("config", currTime);
                    return affected;
                } 
                else
                {
                    int affected = ExecuteWrite(query2, args);
                    UpdateModifiedTime("config", currTime);
                    return affected;
                }
            }
            catch
            {
                WpfMessageBox.Show("Cannot edit user!", MessageBoxType.Error);
                return 0;
            }
        }

        public static int DeleteUser(int id, DateTime currTime)
        {
            const string query = "Delete from users WHERE id=@id";
            var args = new Dictionary<string, object>
            {
                {"@id", id}
            };

            try
            {
                int affected = ExecuteWrite(query, args);
                UpdateModifiedTime("config", currTime);
                return affected;
            }
            catch
            {
                WpfMessageBox.Show("Cannot delete user!", MessageBoxType.Error);
                return 0;
            }
        }

        public static User GetUserByName(string userName)
        {
            var query = "SELECT * FROM users WHERE name=@name";

            var args = new Dictionary<string, object>
            {
                {"@name", userName}
            };

            DataTable dt = ExecuteRead(query, args);

            if (dt == null || dt.Rows.Count == 0)
            {
                return null;
            }

            var user = new User
            {
                id = Convert.ToInt32(dt.Rows[0]["id"]),
                name = Convert.ToString(dt.Rows[0]["name"]),
                password = Convert.ToString(dt.Rows[0]["password"]),
                access = (USER_ACCESS)Convert.ToInt32(dt.Rows[0]["access"]),
                department = Convert.ToInt32(dt.Rows[0]["department"]),
            };

            return user;
        }

        public static List<User> GetUserList()
        {
            var query = "SELECT * FROM users";

            DataTable dt = ExecuteRead(query, null);

            if (dt == null || dt.Rows.Count == 0)
            {
                return null;
            }

            List<User> userList = new List<User>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                var user = new User
                {
                    id = Convert.ToInt32(dt.Rows[i]["id"]),
                    name = Convert.ToString(dt.Rows[i]["name"]),
                    password = Convert.ToString(dt.Rows[i]["password"]),
                    access = (USER_ACCESS)Convert.ToInt32(dt.Rows[i]["access"]),
                    department = Convert.ToInt32(dt.Rows[i]["department"]),
                };
                userList.Add(user);
            }

            return userList;
        }

        public static int AddBuilding(Building building, DateTime currTime)
        {
            const string query = "INSERT INTO buildings(id, name, floors, description) VALUES(@id, @name, @floors, @description)";

            var args = new Dictionary<string, object>
            {
                {"@id", building.id},
                {"@name", building.name},
                {"@floors", building.floors},
                {"@description", building.description}
            };

            try
            {
                int affected = ExecuteWrite(query, args);
                UpdateModifiedTime("config", currTime);
                return affected;
            }
            catch (SQLiteException ex)
            {
                if (ex.Message.StartsWith("constraint failed") && ex.Message.Contains("UNIQUE"))
                {
                    WpfMessageBox.Show("Building exists!", MessageBoxType.Error);
                    return 0;
                }
                else
                {
                    WpfMessageBox.Show("Cannot add Building!", MessageBoxType.Error);
                    return 0;
                }
            }
        }

        public static int UpdateBuilding(Building building, DateTime currTime)
        {
            const string query = "UPDATE buildings SET name=@name, floors=@floors, description=@description WHERE id=@id";

            var args = new Dictionary<string, object>
            {
                {"@id", building.id},
                {"@name", building.name},
                {"@floors", building.floors},
                {"@description", building.description}
            };

            try
            {
                int affected = ExecuteWrite(query, args);
                UpdateModifiedTime("config", currTime);
                return affected;
            }
            catch
            {
                WpfMessageBox.Show("Cannot edit Building!", MessageBoxType.Error);
                return 0;
            }
        }

        public static int DeleteBuilding(int id, DateTime currTime)
        {
            const string query = "Delete from buildings WHERE id=@id";
            var args = new Dictionary<string, object>
            {
                {"@id", id}
            };

            try
            {
                int affected = ExecuteWrite(query, args);
                UpdateModifiedTime("config", currTime);
                return affected;
            }
            catch (SQLiteException ex)
            {
                if (ex.Message.StartsWith("constraint failed") && ex.Message.Contains("FOREIGN KEY"))
                {
                    WpfMessageBox.Show("Rooms of the Building exist!", MessageBoxType.Error);
                    return 0;
                }
                else
                {
                    WpfMessageBox.Show("Cannot delete Building!", MessageBoxType.Error);
                    return 0;
                }
            }
        }

        public static List<Building> GetBuildingList()
        {
            var query = "SELECT * FROM buildings";

            DataTable dt = ExecuteRead(query, null);

            if (dt == null || dt.Rows.Count == 0)
            {
                return null;
            }

            List<Building> buildingList = new List<Building>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                var building = new Building
                {
                    id = Convert.ToInt32(dt.Rows[i]["id"]),
                    name = Convert.ToString(dt.Rows[i]["name"]),
                    floors = Convert.ToInt32(dt.Rows[i]["floors"]),
                    description = Convert.ToString(dt.Rows[i]["description"])
                };
                buildingList.Add(building);
            }

            return buildingList;
        }

        public static int AddRoomType(RoomType roomtype, DateTime currTime)
        {
            const string query = "INSERT INTO room_types(id, name, price, max_cards, description) VALUES(@id, @name, @price, @max_cards, @description)";

            var args = new Dictionary<string, object>
            {
                {"@id", roomtype.id},
                {"@name", roomtype.name},
                {"@price", roomtype.price},
                {"@max_cards", roomtype.max_cards},
                {"@description", roomtype.description}
            };

            try
            {
                int affected = ExecuteWrite(query, args);
                UpdateModifiedTime("config", currTime);
                return affected;
            }
            catch (SQLiteException ex)
            {
                if (ex.Message.StartsWith("constraint failed") && ex.Message.Contains("UNIQUE"))
                {
                    WpfMessageBox.Show("Room Type exists!", MessageBoxType.Error);
                    return 0;
                }
                else
                {
                    WpfMessageBox.Show("Cannot add Room Type!", MessageBoxType.Error);
                    return 0;
                }
            }
        }

        public static int UpdateRoomType(RoomType roomtype, DateTime currTime)
        {
            const string query = "UPDATE room_types SET name=@name, price=@price, max_cards=@max_cards, description=@description WHERE id=@id";

            var args = new Dictionary<string, object>
            {
                {"@id", roomtype.id},
                {"@name", roomtype.name},
                {"@price", roomtype.price},
                {"@max_cards", roomtype.max_cards},
                {"@description", roomtype.description}
            };

            try
            {
                int affected = ExecuteWrite(query, args);
                UpdateModifiedTime("config", currTime);
                return affected;
            }
            catch
            {
                WpfMessageBox.Show("Cannot edit Room Type!", MessageBoxType.Error);
                return 0;
            }
        }

        public static int DeleteRoomType(int id, DateTime currTime)
        {
            const string query = "Delete from room_types WHERE id=@id";
            var args = new Dictionary<string, object>
            {
                {"@id", id}
            };

            try
            {
                int affected = ExecuteWrite(query, args);
                UpdateModifiedTime("config", currTime);
                return affected;
            }
            catch (SQLiteException ex)
            {
                if (ex.Message.StartsWith("constraint failed") && ex.Message.Contains("FOREIGN KEY"))
                {
                    WpfMessageBox.Show("Rooms of the Type exist!", MessageBoxType.Error);
                    return 0;
                }
                else
                {
                    WpfMessageBox.Show("Cannot delete Room Type!", MessageBoxType.Error);
                    return 0;
                }
            }
        }

        public static List<RoomType> GetRoomTypeList()
        {
            var query = "SELECT * FROM room_types";

            DataTable dt = ExecuteRead(query, null);

            if (dt == null || dt.Rows.Count == 0)
            {
                return null;
            }

            List<RoomType> roomTypeList = new List<RoomType>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                var room_type = new RoomType
                {
                    id = Convert.ToInt32(dt.Rows[i]["id"]),
                    name = Convert.ToString(dt.Rows[i]["name"]),
                    price = Convert.ToInt32(dt.Rows[i]["price"]),
                    max_cards = Convert.ToInt32(dt.Rows[i]["max_cards"]),
                    description = Convert.ToString(dt.Rows[i]["description"])
                };
                roomTypeList.Add(room_type);
            }
            return roomTypeList;
        }

        public static int AddArea(Area area, DateTime currTime)
        {
            const string query = "INSERT INTO areas(name, description) VALUES(@name, @description)";

            //here we are setting the parameter values that will be actually 
            //replaced in the query in Execute method
            var args = new Dictionary<string, object>
            {
                {"@name", area.name},
                {"@description", area.description}
            };

            try
            {
                int affected = ExecuteWrite(query, args);
                UpdateModifiedTime("config", currTime);
                return affected;
            }
            catch (SQLiteException ex)
            {
                if (ex.Message.StartsWith("constraint failed"))
                {
                    WpfMessageBox.Show("Area exists!");
                    return 0;
                }
                else
                {
                    WpfMessageBox.Show("Cannot add area!");
                    return 0;
                }
            }
        }

        public static int UpdateArea(Area area, DateTime currTime)
        {
            const string query = "UPDATE areas SET name=@name, description=@description WHERE id=@id";

            var args = new Dictionary<string, object>
            {
                {"@id", area.id},
                {"@name", area.name},
                {"@description", area.description}
            };

            try
            {
                int affected = ExecuteWrite(query, args);
                UpdateModifiedTime("config", currTime);
                return affected;
            }
            catch
            {
                WpfMessageBox.Show("Cannot edit Area!", MessageBoxType.Error);
                return 0;
            }
        }

        public static int DeleteArea(int id, DateTime currTime)
        {
            const string query = "DELETE FROM areas WHERE Id=@id";

            var args = new Dictionary<string, object>
            {
                {"@id", id},
            };

            try
            {
                int affected = ExecuteWrite(query, args);
                UpdateModifiedTime("config", currTime);
                return affected;
            }
            catch (SQLiteException ex)
            {
                if (ex.Message.StartsWith("constraint failed") && ex.Message.Contains("FOREIGN KEY"))
                {
                    WpfMessageBox.Show("Rooms of the Area exist!", MessageBoxType.Error);
                    return 0;
                }
                else
                {
                    WpfMessageBox.Show("Cannot delete Area!", MessageBoxType.Error);
                    return 0;
                }
            }
        }

        public static List<Area> GetAreaList()
        {
            var query = "SELECT * FROM areas";

            DataTable dt = ExecuteRead(query, null);

            if (dt == null || dt.Rows.Count == 0)
            {
                return null;
            }

            List<Area> areaList = new List<Area>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                var area = new Area
                {
                    id = Convert.ToInt32(dt.Rows[i]["id"]),
                    name = Convert.ToString(dt.Rows[i]["name"]),
                    description = Convert.ToString(dt.Rows[i]["description"])
                };
                areaList.Add(area);
            }

            return areaList;
        }

        public static int AddRoom(Room room, DateTime currTime)
        {
            // Get room list and check duplicated rooms
            List<Room> roomList = GetRoomList();
            if (roomList != null)
            {
                foreach (var tmproom in roomList)
                {
                    if (tmproom.room_no == room.room_no)
                    {
                        WpfMessageBox.Show("Room No. exists!", MessageBoxType.Error);
                        return 0;
                    }
                    if (tmproom.full_no == room.full_no)
                    {
                        WpfMessageBox.Show("Room No. or Lock No. is repeated!", MessageBoxType.Error);
                        return 0;
                    }
                }
            }

            // Add room
            const string query = "INSERT INTO rooms(room_no, building, floor, lock_no, room_type, full_no, lock_type, sub, area, price, max_cards, status, passage_mode, hide_room) " +
                "VALUES(@room_no, @building, @floor, @lock_no, @room_type, @full_no, @lock_type, @sub, @area, @price, @max_cards, @status, @passage_mode, @hide_room)";

            var args = new Dictionary<string, object>
            {
                {"@room_no", room.room_no},
                {"@building", room.building},
                {"@floor", room.floor},
                {"@lock_no", room.lock_no},
                {"@room_type", room.room_type},
                {"@full_no", room.full_no},
                {"@lock_type", (int)room.lock_type},
                {"@sub", room.sub},
                {"@area", (room.area == 0) ? DBNull.Value : (object)room.area},
                {"@price", room.price},
                {"@max_cards", room.max_cards},
                {"@status", (int)room.status},
                {"@passage_mode", room.passage_mode ? 1 : 0},
                {"@hide_room", room.hide_room ? 1 : 0}
            };

            try
            {
                int affected = ExecuteWrite(query, args);
                UpdateModifiedTime("config", currTime);
                return affected;
            }
            catch (SQLiteException ex)
            {
                if (ex.Message.StartsWith("constraint failed") && ex.Message.Contains("UNIQUE"))
                {
                    WpfMessageBox.Show("Room exists!", MessageBoxType.Error);
                    return 0;
                }
                else
                {
                    WpfMessageBox.Show("Cannot add Room!", MessageBoxType.Error);
                    return 0;
                }
            }
        }

        public static int UpdateRoom(Room room, DateTime currTime)
        {
            // Get room list and check duplicated rooms
            List<Room> roomList = GetRoomList();
            if (roomList != null)
            {
                foreach (var tmproom in roomList)
                {
                    if (tmproom.room_no == room.room_no && tmproom.id != room.id)
                    {
                        WpfMessageBox.Show("Room No. exists!", MessageBoxType.Error);
                        return 0;
                    }
                    if (tmproom.full_no == room.full_no && tmproom.id != room.id)
                    {
                        WpfMessageBox.Show("Room No. or Lock No. is repeated!", MessageBoxType.Error);
                        return 0;
                    }
                }
            }
            // Update room
            const string query = "UPDATE rooms SET room_no=@room_no, building=@building, floor=@floor, lock_no=@lock_no, " +
                "room_type=@room_type, full_no=@full_no, lock_type=@lock_type, sub=@sub, area=@area, price=@price, max_cards=@max_cards, " +
                "status=@status, passage_mode=@passage_mode, hide_room=@hide_room WHERE id=@id";

            var args = new Dictionary<string, object>
            {
                {"@id", room.id },
                {"@room_no", room.room_no},
                {"@building", room.building},
                {"@floor", room.floor},
                {"@lock_no", room.lock_no},
                {"@room_type", room.room_type},
                {"@full_no", room.full_no},
                {"@lock_type", (int)room.lock_type},
                {"@sub", room.sub},
                {"@area", (room.area == 0) ? DBNull.Value : (object)room.area},
                {"@price", room.price},
                {"@max_cards", room.max_cards},
                {"@status", (int)room.status},
                {"@passage_mode", room.passage_mode ? 1 : 0},
                {"@hide_room", room.hide_room ? 1 : 0}
            };

            try
            {
                int affected = ExecuteWrite(query, args);
                UpdateModifiedTime("config", currTime);
                return affected;
            }
            catch
            {
                WpfMessageBox.Show("Cannot edit Room!", MessageBoxType.Error);
                return 0;
            }
        }

        public static int UpdateRoomStatus(string full_no, ROOM_STATUS status, DateTime currTime)
        {
            const string query = "UPDATE rooms SET status=@status WHERE full_no=@full_no";

            var args = new Dictionary<string, object>
            {
                {"@full_no", full_no},
                {"@status", (int)status},
            };

            try
            {
                int affected = ExecuteWrite(query, args);
                UpdateModifiedTime("config", currTime);
                return affected;
            }
            catch
            {
                WpfMessageBox.Show("Cannot edit Room Status!", MessageBoxType.Error);
                return 0;
            }
        }

        public static int DeleteRoom(int id, DateTime currTime)
        {
            const string query = "Delete from rooms WHERE id=@id";
            var args = new Dictionary<string, object>
            {
                {"@id", id}
            };

            try
            {
                int affected = ExecuteWrite(query, args);
                UpdateModifiedTime("config", currTime);
                return affected;
            }
            catch
            {
                WpfMessageBox.Show("Cannot delete the room!", MessageBoxType.Error);
                return 0;
            }
        }

        public static List<Room> GetRoomList()
        {
            var query = "SELECT * FROM rooms ORDER BY building, floor, room_no";

            DataTable dt = ExecuteRead(query, null);

            return ContsructRoomList(dt);
        }

        public static List<Room> GetVisibleRoomList()
        {
            // Show only Guest room and Sub rooms, excluding hidden rooms
            var query = "SELECT * FROM rooms WHERE hide_room=0 AND (lock_type=0 OR lock_type=1) ORDER BY building, floor, room_no";

            DataTable dt = ExecuteRead(query, null);

            return ContsructRoomList(dt);
        }

        private static List<Room> ContsructRoomList(DataTable dt)
        {
            if (dt == null || dt.Rows.Count == 0)
            {
                return null;
            }
            List<Room> roomList = new List<Room>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                var room = new Room
                {
                    id = Convert.ToInt32(dt.Rows[i]["id"]),
                    room_no = Convert.ToString(dt.Rows[i]["room_no"]),
                    building = Convert.ToInt32(dt.Rows[i]["building"]),
                    floor = Convert.ToInt32(dt.Rows[i]["floor"]),
                    lock_no = Convert.ToInt32(dt.Rows[i]["lock_no"]),
                    room_type = Convert.ToInt32(dt.Rows[i]["room_type"]),
                    lock_type = (LOCK_TYPE)Convert.ToInt32(dt.Rows[i]["lock_type"]),
                    sub = dt.Rows[i]["sub"] == DBNull.Value ? 0 : Convert.ToInt32(dt.Rows[i]["sub"]),
                    area = dt.Rows[i]["area"] == DBNull.Value ? 0 : Convert.ToInt32(dt.Rows[i]["area"]),
                    price = dt.Rows[i]["price"] == DBNull.Value ? 0 : Convert.ToInt32(dt.Rows[i]["price"]),
                    max_cards = Convert.ToInt32(dt.Rows[i]["max_cards"]),
                    status = (ROOM_STATUS)Convert.ToInt32(dt.Rows[i]["status"]),
                    passage_mode = Convert.ToBoolean(dt.Rows[i]["passage_mode"]),
                    hide_room = Convert.ToBoolean(dt.Rows[i]["hide_room"]),
                };

                roomList.Add(room);
            }
            return roomList;
        }

        public static List<RoomStatusItem> GetRoomStatusList()
        {
            var query = "SELECT rooms.id, rooms.room_no, rooms.full_no, room_types.name, rooms.status FROM rooms INNER JOIN room_types ON rooms.room_type=room_types.id  ORDER BY building, floor, room_no";
            DataTable dt = ExecuteRead(query, null);
            if (dt == null || dt.Rows.Count == 0)
            {
                return null;
            }

            List<RoomStatusItem> roomStatusList = new List<RoomStatusItem>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                var roomStatusItem = new RoomStatusItem
                {
                    id = Convert.ToInt32(dt.Rows[i]["id"]),
                    room_no = Convert.ToString(dt.Rows[i]["room_no"]),
                    full_no = Convert.ToString(dt.Rows[i]["full_no"]),
                    room_type = Convert.ToString(dt.Rows[i]["name"]),
                    status = (ROOM_STATUS)Convert.ToInt32(dt.Rows[i]["status"])
                };

                roomStatusList.Add(roomStatusItem);
            }

            return roomStatusList;
        }

        public static List<RoomInfoItem> GetRoomInfoList()
        {
            var query = "select buildings.id as building_no, buildings.name as building_name, rooms.floor, rooms.room_no, room_types.name as room_type, " +
                "rooms.full_no, rooms.lock_type, areas.name as area, rooms.passage_mode, " +
                "rooms.price, rooms.max_cards, rooms.hide_room from rooms INNER JOIN buildings ON rooms.building=buildings.id " +
                "INNER JOIN room_types ON rooms.room_type=room_types.id LEFT JOIN areas ON areas.id=rooms.area " +
                "ORDER BY building, floor, room_no";

            DataTable dt = ExecuteRead(query, null);
            if (dt == null || dt.Rows.Count == 0)
            {
                return null;
            }

            List<RoomInfoItem> roomInfoList = new List<RoomInfoItem>();

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                var roomInfoItem = new RoomInfoItem
                {
                    building_no = string.Format("{0,3:D3}", Convert.ToInt32(dt.Rows[i]["building_no"])),
                    building_name = Convert.ToString(dt.Rows[i]["building_name"]),
                    floor = string.Format("{0,3:D3}", Convert.ToInt32(dt.Rows[i]["floor"])),
                    room_no = Convert.ToString(dt.Rows[i]["room_no"]),
                    room_type = Convert.ToString(dt.Rows[i]["room_type"]),
                    full_no = Convert.ToString(dt.Rows[i]["full_no"]),
                    lock_type = Defines.LockTypeString[Convert.ToInt32(dt.Rows[i]["lock_type"])],
                    area = dt.Rows[i]["area"] == DBNull.Value ? "" : Convert.ToString(dt.Rows[i]["area"]),
                    passage_mode = Convert.ToBoolean(dt.Rows[i]["passage_mode"]),
                    price = dt.Rows[i]["price"] == DBNull.Value ? 0 : Convert.ToInt32(dt.Rows[i]["price"]),
                    max_cards = Convert.ToInt32(dt.Rows[i]["max_cards"]),
                    hide_room = Convert.ToBoolean(dt.Rows[i]["hide_room"]),
                };

                roomInfoList.Add(roomInfoItem);
            }

            return roomInfoList;
        }

        public static int AddCardLog(CardLog cardLog)
        {
            const string query = "INSERT INTO card_log(log_time, issued_time, card_no, card_type, room_list, area_list, valid_from, valid_until, issuer, holder, flag, blocked, blocked_time) " +
                "VALUES(@log_time, @issued_time, @card_no, @card_type, @room_list, @area_list, @valid_from, @valid_until, @issuer, @holder, @flag, @blocked, @blocked_time)";

            //here we are setting the parameter values that will be actually 
            //replaced in the query in Execute method
            var args = new Dictionary<string, object>
            {
                {"@log_time", cardLog.log_time.ToString("yyyy-MM-dd HH:mm:ss.fff")},
                {"@issued_time", cardLog.issued_time.ToString("yyyy-MM-dd HH:mm:ss.fff")},
                {"@card_no", cardLog.card_no},
                {"@card_type", cardLog.card_type},
                {"@room_list", cardLog.room_list ?? ""},
                {"@area_list", cardLog.area_list ?? ""},
                {"@valid_from", cardLog.valid_from == null ? "" : cardLog.valid_from.ToString("yyyy-MM-dd HH:mm:ss")},
                {"@valid_until", cardLog.valid_until == null ? "" : cardLog.valid_until.ToString("yyyy-MM-dd HH:mm:ss")},
                {"@issuer", cardLog.issuer ?? ""},
                {"@holder", cardLog.holder ?? ""},
                {"@flag", cardLog.flag},
                {"@blocked", cardLog.blocked},
                {"@blocked_time", cardLog.blocked_time == null ? "" : cardLog.blocked_time.ToString("yyyy-MM-dd HH:mm:ss")}
            };

            try
            {
                int affected = ExecuteWrite(query, args);
                UpdateModifiedTime("log", cardLog.log_time);
                return affected;
            }
            catch
            {
                WpfMessageBox.Show("Cannot add log!");
                return 0;
            }
        }

        public static CardLog FindGuestKeyForRoom(string room_no, DateTime currTime)
        {
            const string query = "SELECT * from card_log WHERE card_type=1 AND blocked=0 AND room_list=@room_no AND DATETIME(valid_until) > DATETIME(@currTime)" +
                "ORDER BY DATETIME(issued_time) DESC";

            var args = new Dictionary<string, object>
            {
                {"@room_no", room_no},
                {"@currTime", currTime.ToString("yyyy-MM-dd HH:mm:ss")},
            };

            DataTable dt = ExecuteRead(query, args);

            if (dt == null || dt.Rows.Count == 0)
            {
                return null;
            }

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                var foundCard = new CardLog
                {
                    log_time = DateTime.Parse(Convert.ToString(dt.Rows[i]["log_time"])),
                    issued_time = DateTime.Parse(Convert.ToString(dt.Rows[i]["issued_time"])),
                    card_no = Convert.ToString(dt.Rows[i]["card_no"]),
                    card_type = Convert.ToInt32(dt.Rows[i]["card_type"]),
                    room_list = Convert.ToString(dt.Rows[i]["room_list"]),
                    area_list = Convert.ToString(dt.Rows[i]["area_list"]),
                    valid_from = DateTime.Parse(Convert.ToString(dt.Rows[i]["valid_from"])),
                    valid_until = DateTime.Parse(Convert.ToString(dt.Rows[i]["valid_until"])),
                    issuer = Convert.ToString(dt.Rows[i]["issuer"]),
                    holder = Convert.ToString(dt.Rows[i]["holder"]),
                    flag = Convert.ToInt32(dt.Rows[i]["flag"]),
                    blocked = Convert.ToInt32(dt.Rows[i]["blocked"]),
                    blocked_time = DateTime.Parse(Convert.ToString(dt.Rows[i]["blocked_time"])),
                };

                if (GetKeyBlockedState(foundCard.card_no) == 0) // checked in
                {
                    return foundCard;
                }
            }
            return null;
        }

        public static CardLog FindConflictingGuestKeyForRoom(string room_no, DateTime currTime)
        {
            const string query = "SELECT * from card_log WHERE card_type=1 AND blocked=0 AND room_list=@room_no AND DATETIME(valid_from) > DATETIME(@currTime)" +
                "ORDER BY DATETIME(issued_time) DESC";

            var args = new Dictionary<string, object>
            {
                {"@room_no", room_no},
                {"@currTime", currTime.ToString("yyyy-MM-dd HH:mm:ss")},
            };

            DataTable dt = ExecuteRead(query, args);

            if (dt == null || dt.Rows.Count == 0)
            {
                return null;
            }

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                var foundCard = new CardLog
                {
                    log_time = DateTime.Parse(Convert.ToString(dt.Rows[i]["log_time"])),
                    issued_time = DateTime.Parse(Convert.ToString(dt.Rows[i]["issued_time"])),
                    card_no = Convert.ToString(dt.Rows[i]["card_no"]),
                    card_type = Convert.ToInt32(dt.Rows[i]["card_type"]),
                    room_list = Convert.ToString(dt.Rows[i]["room_list"]),
                    area_list = Convert.ToString(dt.Rows[i]["area_list"]),
                    valid_from = DateTime.Parse(Convert.ToString(dt.Rows[i]["valid_from"])),
                    valid_until = DateTime.Parse(Convert.ToString(dt.Rows[i]["valid_until"])),
                    issuer = Convert.ToString(dt.Rows[i]["issuer"]),
                    holder = Convert.ToString(dt.Rows[i]["holder"]),
                    flag = Convert.ToInt32(dt.Rows[i]["flag"]),
                    blocked = Convert.ToInt32(dt.Rows[i]["blocked"]),
                    blocked_time = DateTime.Parse(Convert.ToString(dt.Rows[i]["blocked_time"])),
                };

                if (GetKeyBlockedState(foundCard.card_no) == 0) // checked in
                {
                    return foundCard;
                }
            }
            return null;
        }

        public static int Count_Key(string room_no, DateTime checkin_time, DateTime checkout_time)
        {
            const string query = "SELECT COUNT(issued_time) AS count_key FROM card_log WHERE room_list=@room_no AND blocked=0 AND DATETIME(valid_from)=@checkin_time AND DATETIME(valid_until)=@checkout_time";

            var args = new Dictionary<string, object>
            {
                {"@room_no", room_no},
                {"@checkin_time", checkin_time.ToString("yyyy-MM-dd HH:mm:ss")},
                {"@checkout_time", checkout_time.ToString("yyyy-MM-dd HH:mm:ss")},
            };

            DataTable dt = ExecuteRead(query, args);

            try
            {
                return Convert.ToInt32(dt.Rows[0]["count_key"]);
            }
            catch
            {
                return 0;
            }
        }

        public static int GetKeyBlockedState(string card_no)
        {
            // Get latest state of key
            const string query = "SELECT blocked from card_log WHERE card_no=@card_no ORDER BY DATETIME(issued_time) DESC";
            var args = new Dictionary<string, object>
            {
                {"@card_no", card_no},
            };

            DataTable dt = ExecuteRead(query, args);

            if (dt == null || dt.Rows.Count == 0)
            {
                return -1;
            }
            return Convert.ToInt32(dt.Rows[0]["blocked"]);
        }

        public static string GetCardHolder(string card_no)
        {
            // Get latest state of key
            const string query = "SELECT holder from card_log WHERE card_no=@card_no ORDER BY DATETIME(issued_time) DESC";
            var args = new Dictionary<string, object>
            {
                {"@card_no", card_no},
            };

            DataTable dt = ExecuteRead(query, args);

            if (dt == null || dt.Rows.Count == 0)
            {
                return "";
            }
            return Convert.ToString(dt.Rows[0]["holder"]);
        }

        public static int Checkout_Room(string full_no)
        {
            // update room status
            DateTime currTime = DateTime.Now;
            int ret = UpdateRoomStatus(full_no, ROOM_STATUS.VAC, currTime);
            if (ret == 0)
            {
                return 0;
            }
            // block old keys
            ret = BlockOldKeys(full_no, currTime);
            return ret;
        }

        public static int BlockOldKeys(string full_no, DateTime currTime)
        {
            const string query = "SELECT * from card_log WHERE card_type=1 AND blocked=0 AND room_list=@full_no AND DATETIME(valid_until) > DATETIME(@currTime) " +
                "ORDER BY DATETIME(issued_time) DESC";

            var args = new Dictionary<string, object>
            {
                {"@full_no", full_no},
                {"@currTime", currTime.ToString("yyyy-MM-dd HH:mm:ss")},
            };

            DataTable dt = ExecuteRead(query, args);

            if (dt == null || dt.Rows.Count == 0)
            {
                return 0;
            }

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                var foundCard = new CardLog
                {
                    log_time = DateTime.Parse(Convert.ToString(dt.Rows[i]["log_time"])),
                    issued_time = DateTime.Parse(Convert.ToString(dt.Rows[i]["issued_time"])),
                    card_no = Convert.ToString(dt.Rows[i]["card_no"]),
                    card_type = Convert.ToInt32(dt.Rows[i]["card_type"]),
                    room_list = Convert.ToString(dt.Rows[i]["room_list"]),
                    area_list = Convert.ToString(dt.Rows[i]["area_list"]),
                    valid_from = DateTime.Parse(Convert.ToString(dt.Rows[i]["valid_from"])),
                    valid_until = DateTime.Parse(Convert.ToString(dt.Rows[i]["valid_until"])),
                    issuer = Convert.ToString(dt.Rows[i]["issuer"]),
                    holder = Convert.ToString(dt.Rows[i]["holder"]),
                    flag = Convert.ToInt32(dt.Rows[i]["flag"]),
                    blocked = Convert.ToInt32(dt.Rows[i]["blocked"]),
                };

                if (GetKeyBlockedState(foundCard.card_no) == 0) // checked in
                {
                    foundCard.blocked = 1;  // set to blocked
                    foundCard.blocked_time = currTime;  // set checkout time
                    if (AddCardLog(foundCard) == 0) {
                        return 0;
                    }
                }
            }
            return 1;
        }

        public static int CancelKey(string user, string card_no, DateTime currTime)
        {
            // Get latest state of key
            const string query = "SELECT * from card_log WHERE card_no=@card_no ORDER BY DATETIME(issued_time) DESC";
            var args = new Dictionary<string, object>
            {
                {"@card_no", card_no},
            };

            DataTable dt = ExecuteRead(query, args);

            if (dt == null || dt.Rows.Count == 0)
            {
                var foundCard = new CardLog
                {
                    log_time = currTime,
                    issued_time = currTime,
                    card_no = card_no,
                    card_type = Defines.EMPTY_CARD,
                    room_list = "",
                    area_list = "",
                    valid_from = currTime,
                    valid_until = currTime,
                    issuer = user,
                    holder = "",
                    flag = 0,
                    blocked = 2,
                    blocked_time = currTime
                };
                return AddCardLog(foundCard);
            }
            else
            {
                var foundCard = new CardLog
                {
                    log_time = currTime,
                    issued_time = DateTime.Parse(Convert.ToString(dt.Rows[0]["issued_time"])),
                    card_no = Convert.ToString(dt.Rows[0]["card_no"]),
                    card_type = Convert.ToInt32(dt.Rows[0]["card_type"]),
                    room_list = Convert.ToString(dt.Rows[0]["room_list"]),
                    area_list = Convert.ToString(dt.Rows[0]["area_list"]),
                    valid_from = DateTime.Parse(Convert.ToString(dt.Rows[0]["valid_from"])),
                    valid_until = DateTime.Parse(Convert.ToString(dt.Rows[0]["valid_until"])),
                    issuer = user,
                    holder = Convert.ToString(dt.Rows[0]["holder"]),
                    flag = Convert.ToInt32(dt.Rows[0]["flag"]),
                    blocked = 2,
                    blocked_time = currTime
                };
                return AddCardLog(foundCard);

            }
        }

        public static List<KeyCreationReportItem> GetKeyCreationReport(DateTime startTime, DateTime endTime, List<string> operatorList, List<int> cardTypeList, string roomNo, string cardNo)
        {
            string query = "SELECT * from card_log WHERE DATETIME(issued_time) >= DATETIME(@startTime) AND DATETIME(issued_time) < DATETIME(@endTime)";

            // operatorlist
            if (operatorList != null && operatorList.Count > 0)
            {
                query += " AND (";
                int i = 0;
                foreach (string op in operatorList)
                {
                    if (i > 0) query += " OR ";
                    query += string.Format("issuer='{0}'", op);
                    i++;
                }
                query += ")";
            }

            // cardTypeList
            if (cardTypeList != null && cardTypeList.Count > 0)
            {
                query += " AND (";
                int i = 0;
                foreach (int ct in cardTypeList)
                {
                    if (i > 0) query += " OR ";
                    query += string.Format("card_type='{0}'", ct);
                    i++;
                }
                query += ")";
            }

            // roomNo
            if (roomNo != null && roomNo.Length > 0)
            {
                query += " AND (";
                query += string.Format("room_list LIKE '%{0}%'", roomNo);
                query += ")";
            }

            // cardNo
            if (cardNo != null && cardNo.Length > 0)
            {
                query += " AND (";
                query += string.Format("card_no='{0}'", cardNo);
                query += ")";
            }

            // order
            query += " ORDER BY DATETIME(issued_time) DESC";

            var args = new Dictionary<string, object>
            {
                {"@startTime", startTime.ToString("yyyy-MM-dd HH:mm:ss")},
                {"@endTime", endTime.ToString("yyyy-MM-dd HH:mm:ss")},
            };

            List<KeyCreationReportItem> reportData = new List<KeyCreationReportItem>();
            try
            {
                DataTable dt = ExecuteRead(query, args);

                if (dt == null || dt.Rows.Count == 0)
                {
                    return null;
                }

                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    string roomStr = (Convert.ToString(dt.Rows[i]["room_list"]).Split(' '))[0];
                    string[] roomStrLst = roomStr.Split('.');
                    int blocked = Convert.ToInt32(dt.Rows[i]["blocked"]);
                    if (blocked == 0)
                    {
                        // search return time and c/o time
                        string checkout_time = "";
                        string return_time = "";
                        for (int j = 0; j < dt.Rows.Count; j++)
                        {
                            if (Convert.ToInt32(dt.Rows[i]["card_type"]) == Convert.ToInt32(dt.Rows[j]["card_type"]) &&
                                Convert.ToString(dt.Rows[i]["card_no"]) == Convert.ToString(dt.Rows[j]["card_no"]) &&
                                Convert.ToString(dt.Rows[i]["issued_time"]) == Convert.ToString(dt.Rows[j]["issued_time"]))
                            {
                                if (Convert.ToInt32(dt.Rows[j]["blocked"]) == 1)   // Checkout
                                {
                                    checkout_time = DateTime.Parse(Convert.ToString(dt.Rows[j]["blocked_time"])).ToString("M/d/yyyy HH:mm");
                                }
                                if (Convert.ToInt32(dt.Rows[j]["blocked"]) == 2)   // Cancel/Return Key
                                {
                                    return_time = DateTime.Parse(Convert.ToString(dt.Rows[j]["blocked_time"])).ToString("M/d/yyyy HH:mm");
                                }
                            }
                        }

                        var reportItem = new KeyCreationReportItem
                        {
                            CardType = Defines.ACardType[Convert.ToInt32(dt.Rows[i]["card_type"])],
                            CardNo = Convert.ToString(dt.Rows[i]["card_no"]),
                            Issuer = Convert.ToString(dt.Rows[i]["issuer"]),
                            BuildingNo = roomStrLst.Length > 0 ? roomStrLst[0] : "",
                            FloorNo = roomStrLst.Length > 1 ? roomStrLst[1] : "",
                            RoomNo = roomStrLst.Length > 2 ? roomStrLst[2] : "",
                            IssuedTime = DateTime.Parse(Convert.ToString(dt.Rows[i]["issued_time"])).ToString("M/d/yyyy HH:mm:ss"),
                            ReturnedTime = return_time,
                            CheckoutTime = checkout_time,
                            CardHolder = Convert.ToString(dt.Rows[i]["holder"])
                        };
                        reportData.Add(reportItem);
                    }
                }
            }
            catch
            {
                WpfMessageBox.Show("Error in filter!");
            }
            return reportData;
        }

        public static List<MultiDoorKeyReportItem> GetMultiDoorKeyReport(DateTime startTime, DateTime endTime, List<string> keyHolderList, string roomNo)
        {
            string query = "SELECT * from card_log WHERE DATETIME(issued_time) >= DATETIME(@startTime) AND DATETIME(issued_time) < DATETIME(@endTime)";

            // operatorlist
            if (keyHolderList != null && keyHolderList.Count > 0)
            {
                query += " AND (";
                int i = 0;
                foreach (string op in keyHolderList)
                {
                    if (i > 0) query += " OR ";
                    query += string.Format("holder='{0}'", op);
                    i++;
                }
                query += ")";
            }

            // card type
            query += string.Format(" AND card_type={0}", Defines.STAFF_CARD);

            // not blocked
            query += " AND blocked=0";

            // roomNo
            if (roomNo != null && roomNo.Length > 0)
            {
                query += " AND (";
                query += string.Format("room_list LIKE '%{0}%'", roomNo);
                query += ")";
            }

            // order
            query += " ORDER BY DATETIME(issued_time) DESC";

            var args = new Dictionary<string, object>
            {
                {"@startTime", startTime.ToString("yyyy-MM-dd HH:mm:ss")},
                {"@endTime", endTime.ToString("yyyy-MM-dd HH:mm:ss")},
            };

            List<MultiDoorKeyReportItem> reportData = new List<MultiDoorKeyReportItem>();
            try
            {
                DataTable dt = ExecuteRead(query, args);

                if (dt == null || dt.Rows.Count == 0)
                {
                    return null;
                }

                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    int flag = Convert.ToInt32(dt.Rows[i]["flag"]);
                    var reportItem = new MultiDoorKeyReportItem
                    {
                        CardNo = Convert.ToString(dt.Rows[i]["card_no"]),
                        Issuer = Convert.ToString(dt.Rows[i]["issuer"]),
                        Rooms = Convert.ToString(dt.Rows[i]["room_list"]),
                        Areas = Convert.ToString(dt.Rows[i]["area_list"]),
                        IssuedTime = DateTime.Parse(Convert.ToString(dt.Rows[i]["issued_time"])).ToString("M/d/yyyy HH:mm:ss"),
                        ValidUntil = DateTime.Parse(Convert.ToString(dt.Rows[i]["valid_until"])).ToString("M/d/yyyy HH:mm"),
                        CardHolder = Convert.ToString(dt.Rows[i]["holder"]),
                        OpenDeadBolt = (flag & Defines.CF_BACK_LOCK_EN) > 0,
                        PassageMode = (flag & Defines.CF_CHANGE_FLAGS) > 0,
                    };
                    reportData.Add(reportItem);
                }
            }
            catch
            {
                WpfMessageBox.Show("Error in filter!");
            }
            return reportData;
        }

        public static List<KeyHolderReportItem> GetKeyHolderReport(DateTime startTime, DateTime endTime)
        {
            string query = "SELECT * from card_log WHERE DATETIME(issued_time) >= DATETIME(@startTime) AND DATETIME(issued_time) < DATETIME(@endTime)";
            
            // card Holder exists, not blocked
            query += " AND holder<>'' AND blocked=0";
            
            // order
            query += " ORDER BY DATETIME(issued_time) DESC";

            var args = new Dictionary<string, object>
            {
                {"@startTime", startTime.ToString("yyyy-MM-dd HH:mm:ss")},
                {"@endTime", endTime.ToString("yyyy-MM-dd HH:mm:ss")},
            };

            List<KeyHolderReportItem> reportData = new List<KeyHolderReportItem>();
            try
            {
                DataTable dt = ExecuteRead(query, args);

                if (dt == null || dt.Rows.Count == 0)
                {
                    return null;
                }

                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    string roomStr = (Convert.ToString(dt.Rows[i]["room_list"]).Split(' '))[0];
                    string[] roomStrLst = roomStr.Split('.');
                    var reportItem = new KeyHolderReportItem
                    {
                        CardType = Defines.ACardType[Convert.ToInt32(dt.Rows[i]["card_type"])],
                        CardNo = Convert.ToString(dt.Rows[i]["card_no"]),
                        CardHolder = Convert.ToString(dt.Rows[i]["holder"]),
                        BuildingNo = roomStrLst.Length > 0 ? roomStrLst[0] : "",
                        FloorNo = roomStrLst.Length > 1 ? roomStrLst[1] : "",
                        RoomNo = roomStrLst.Length > 2 ? roomStrLst[2] : "",
                        IssuedTime = DateTime.Parse(Convert.ToString(dt.Rows[i]["issued_time"])).ToString("M/d/yyyy HH:mm:ss"),
                        ValidUntil = DateTime.Parse(Convert.ToString(dt.Rows[i]["valid_until"])).ToString("M/d/yyyy HH:mm"),
                    };
                    reportData.Add(reportItem);
                }
            }
            catch
            {
                WpfMessageBox.Show("Error in filter!");
            }
            return reportData;
        }

        public static List<OperatorReportItem> GetOperatorReport(DateTime startTime, DateTime endTime, List<string> operatorList)
        {
            string query = "SELECT * from card_log WHERE DATETIME(issued_time) >= DATETIME(@startTime) AND DATETIME(issued_time) < DATETIME(@endTime)";

            // operatorlist
            if (operatorList != null && operatorList.Count > 0)
            {
                query += " AND (";
                int i = 0;
                foreach (string op in operatorList)
                {
                    if (i > 0) query += " OR ";
                    query += string.Format("issuer='{0}'", op);
                    i++;
                }
                query += ")";
            }

            // order
            query += " ORDER BY DATETIME(issued_time) DESC";

            var args = new Dictionary<string, object>
            {
                {"@startTime", startTime.ToString("yyyy-MM-dd HH:mm:ss")},
                {"@endTime", endTime.ToString("yyyy-MM-dd HH:mm:ss")},
            };

            List<OperatorReportItem> reportData = new List<OperatorReportItem>();
            try
            {
                DataTable dt = ExecuteRead(query, args);

                if (dt == null || dt.Rows.Count == 0)
                {
                    return null;
                }

                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    string roomStr = (Convert.ToString(dt.Rows[i]["room_list"]).Split(' '))[0];
                    string[] roomStrLst = roomStr.Split('.');
                    int blocked = Convert.ToInt32(dt.Rows[i]["blocked"]);
                    if (blocked == 0)
                    {
                        // search blocked status
                        for (int j = 0; j < dt.Rows.Count; j++)
                        {
                            if (Convert.ToInt32(dt.Rows[i]["card_type"]) == Convert.ToInt32(dt.Rows[j]["card_type"]) &&
                                Convert.ToString(dt.Rows[i]["card_no"]) == Convert.ToString(dt.Rows[j]["card_no"]) &&
                                Convert.ToString(dt.Rows[i]["issued_time"]) == Convert.ToString(dt.Rows[j]["issued_time"]) &&
                                Convert.ToInt32(dt.Rows[j]["blocked"]) > 0)
                            {
                                blocked = Convert.ToInt32(dt.Rows[j]["blocked"]);
                                break;
                            }

                        }

                        string status = "";
                        switch (blocked)
                        {
                            case 0:
                                if (DateTime.Now < DateTime.Parse(Convert.ToString(dt.Rows[i]["valid_until"])))
                                {
                                    status = "Expired";
                                }
                                else
                                {
                                    status = "Being used";
                                }
                                break;
                            case 1:
                                status = "Checked out without card";
                                break;
                            case 2:
                                status = "Cancelled";
                                break;
                        }

                        var reportItem = new OperatorReportItem
                        {
                            Issuer = Convert.ToString(dt.Rows[i]["issuer"]),
                            CardType = Defines.ACardType[Convert.ToInt32(dt.Rows[i]["card_type"])],
                            CardNo = Convert.ToString(dt.Rows[i]["card_no"]),
                            IssuedTime = DateTime.Parse(Convert.ToString(dt.Rows[i]["issued_time"])).ToString("M/d/yyyy HH:mm:ss"),
                            CardStatus = status,
                        };
                        reportData.Add(reportItem);
                    }
                }
            }
            catch (SQLiteException ex)
            {
                WpfMessageBox.Show("Error in filter!");
            }
            return reportData;
        }
    }
}
