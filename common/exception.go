package common

import "runtime"

// CurrentFunctionName用于打印调用这个函数的函数的名字
// 这有助于在出现异常时分析是哪里出了问题
func CurFuncName() string {
	pc := make([]uintptr, 1)
	runtime.Callers(2, pc)
	f := runtime.FuncForPC(pc[0])
	return f.Name()
}
func Boom() {
	panic("you must've written something wrong")
}
