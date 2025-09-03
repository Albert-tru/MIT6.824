package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk //通过它访问Put()和Get()
	// 接口定义在kvtest包，实现定义在kvsrv包

	key string

	// 唯一的客户端标识，用于在值里标注持有者？？？
	id string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
// ck：与键值服务器通信的客户端
// l： 这个锁对应的键
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {

	lk := &Lock{ck: ck, key: l, id: kvtest.RandValue(8)}

	return lk
}

// 使用lk和K/V服务器交互
func (lk *Lock) Acquire() {
	// 阻塞式（等待）地获取锁
	for {
		// 1. 获取当前锁的状态和值(值表示持有者ID)，以及版本号
		val, ver, err := lk.ck.Get(lk.key)

		// 2. 根据状态决定下一步操作
		if err == rpc.ErrNoKey {
			// 锁不存在：尝试用版本0创建并占有（值为自身id）
			perr := lk.ck.Put(lk.key, lk.id, 0)
			if perr == rpc.OK {
				return
			}
			if perr == rpc.ErrMaybe {
				// 不确定是否成功，读取确认
				v2, _, e2 := lk.ck.Get(lk.key)
				if e2 == rpc.OK && v2 == lk.id {
					return
				}
			}
			// 其他情况：可能被别人抢先，继续循环
		} else if err == rpc.OK {
			// 锁存在
			if val == lk.id {
				// 已经是自己持有
				return
			}
			if val == "" {
				// 空闲：尝试基于当前版本号占有
				perr := lk.ck.Put(lk.key, lk.id, ver)
				if perr == rpc.OK {
					return
				}
				// 不确定是否成功，读取确认
				if perr == rpc.ErrMaybe {
					v2, _, e2 := lk.ck.Get(lk.key)
					if e2 == rpc.OK && v2 == lk.id {
						return
					}
				}
				// 若失败，说明被竞争，继续等待
			}
			// val 非空且不是自己：被别人持有，等待
		}
		// 3. 为避免忙等待，短暂休眠
		time.Sleep(10 * time.Millisecond)
	}
}

// 读取Get-检查Check-写回Put
func (lk *Lock) Release() {
	// 为了健壮性，Release 也应该在一个循环中进行，以处理网络错误或竞态条件。
	// 这个循环通常只会执行一次。
	for {
		// 1. 获取当前锁的状态和值(持有者)和版本号
		val, ver, err := lk.ck.Get(lk.key)

		// 如果Get失败（如网络错误），短暂休眠后重试
		if err != rpc.OK {
			//time.Sleep(10 * time.Millisecond)
			//continue
		}

		// 如果当前不由自己持有（可能为空或他人），视为已释放或不需要释放
		if val != lk.id {
			return
		}

		// 尝试将值置为空字符串，表示释放
		perr := lk.ck.Put(lk.key, "", ver)
		if perr == rpc.OK {
			return
		}
		if perr == rpc.ErrMaybe {
			v2, _, e2 := lk.ck.Get(lk.key)
			if e2 == rpc.OK && v2 == "" {
				return
			}
		}

		// 可能有竞争，稍后重试
		time.Sleep(10 * time.Millisecond)
	}
}
