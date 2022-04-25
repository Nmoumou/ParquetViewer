package main

import (
	"encoding/csv"
	"fmt"
	"net/url"
	"os"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/data/binding"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/storage"
	"fyne.io/fyne/v2/widget"
	"github.com/adrg/sysfont"
)

type ParquetInfo struct {
	filePath       string              // 文件路径
	schemaName     string              // 实际parquet名称
	schemaDetial   string              //详细表定义
	recordsum      int                 // 记录总数
	recordmaxlen   []int               //每列最长的内容数
	recordtile     []string            //列标题
	recordcontents []map[string]string //记录内容
}

var topWindow fyne.Window
var data = [][]string{}
var datalen []int

type MiddleContent struct {
	Title string
	View  func(w fyne.Window) fyne.CanvasObject
}

type MakeNavPara struct {
	welcomMiddle     MiddleContent
	tableMiddle      MiddleContent
	schemaMiddle     MiddleContent
	label_SchemaName binding.String
	label_RecrodSum  binding.Int
	list_SchemaList  binding.StringList
	fd               *dialog.FileDialog
	viewtable        *widget.Table
	schemadeatil     binding.String
}

func main() {
	parquetInfo := ParquetInfo{}
	makeNavPara := MakeNavPara{}
	makeNavPara.label_SchemaName = binding.BindString(&parquetInfo.schemaName)
	makeNavPara.label_RecrodSum = binding.BindInt(&parquetInfo.recordsum)
	makeNavPara.list_SchemaList = binding.BindStringList(&parquetInfo.recordtile)
	makeNavPara.schemadeatil = binding.BindString(&parquetInfo.schemaDetial)
	//寻找系统字体
	finder := sysfont.NewFinder(nil)
	fontArray := []string{}
	fontMap := make(map[string]string)
	for _, font := range finder.List() {
		if font.Family != "" && font.Name != "" {
			//设置系统字体
			fontArray = append(fontArray, font.Name)
			fontMap[font.Name] = font.Filename
		}
	}
	//windows默认设置一微软雅黑字体
	os.Setenv("FYNE_FONT", fontMap["Microsoft YaHei"])

	myApp := app.New()
	myWindow := myApp.NewWindow("ParquetViewer")
	topWindow = myWindow

	content := container.NewMax()
	title := widget.NewLabel("Component name")

	setMiddleContent := func(mc MiddleContent) {
		if fyne.CurrentDevice().IsMobile() {
			child := myApp.NewWindow(mc.Title)
			topWindow = child
			child.SetContent(mc.View(topWindow))
			child.Show()
			child.SetOnClosed(func() {
				topWindow = myWindow
			})
			return
		}

		title.SetText(mc.Title)

		content.Objects = []fyne.CanvasObject{mc.View(myWindow)}
		content.Refresh()
	}

	viewtable := widget.NewTable(
		func() (int, int) {
			if data != nil {
				return len(data), len(data[0])
			} else {
				return 0, 0
			}

		},
		func() fyne.CanvasObject {
			return widget.NewLabel("wide content")
		},
		func(i widget.TableCellID, o fyne.CanvasObject) {
			o.(*widget.Label).SetText(data[i.Row][i.Col])
		})
	makeNavPara.viewtable = viewtable
	//欢迎界面
	welcomMiddle := MiddleContent{
		Title: "Welcome",
		View: func(w fyne.Window) fyne.CanvasObject {
			return container.NewCenter(container.NewVBox(
				widget.NewLabelWithStyle("Welcome to the ParquetViewer APP", fyne.TextAlignCenter, fyne.TextStyle{Bold: true}),
				container.NewHBox(
					widget.NewLabel("Designed by"),
					widget.NewLabel("-"),
					widget.NewHyperlink("Nmoumou", parseURL("https://github.com/Nmoumou")),
				),
			))
		},
	}
	makeNavPara.welcomMiddle = welcomMiddle
	//表格界面
	tableMiddle := MiddleContent{
		Title: "Table",
		View: func(w fyne.Window) fyne.CanvasObject {
			describelabel := widget.NewLabel("This is only show 20 records for view")
			return container.NewBorder(describelabel, nil, nil, nil, describelabel, makeNavPara.viewtable)
		},
	}
	makeNavPara.tableMiddle = tableMiddle

	//Schema界面
	schemaMiddle := MiddleContent{
		Title: "Schema",
		View: func(w fyne.Window) fyne.CanvasObject {
			entry_schema := widget.NewEntryWithData(makeNavPara.schemadeatil)
			return container.NewBorder(nil, nil, nil, nil, entry_schema)
		},
	}
	makeNavPara.schemaMiddle = schemaMiddle
	// 打开文件对话框
	fd := dialog.NewFileOpen(func(reader fyne.URIReadCloser, err error) {
		if err != nil {
			dialog.ShowError(err, myWindow)
			return
		}
		if reader == nil {
			fmt.Println("Cancelled choose")
		} else {
			dialogPro := dialog.NewCustom("Loading", "OK", container.New(layout.NewVBoxLayout(), widget.NewLabel("Please be patient, the data is loading"), widget.NewProgressBarInfinite()), myWindow)
			dialogPro.Show()
			//初始化数据
			parquetInfo.recordtile = []string{}
			parquetInfo.recordcontents = []map[string]string{}
			parquetInfo.recordmaxlen = []int{}
			parquetInfo.recordsum = 0

			parse(reader.URI().Path(), &parquetInfo)
			makeNavPara.label_SchemaName.Set(parquetInfo.schemaName)
			makeNavPara.label_RecrodSum.Set(parquetInfo.recordsum)
			makeNavPara.list_SchemaList.Set(parquetInfo.recordtile)
			makeNavPara.schemadeatil.Set(parquetInfo.schemaDetial)
			//更新表格数据
			data = make([][]string, len(parquetInfo.recordcontents))
			//列名
			for j := 0; j < len(parquetInfo.recordtile); j++ {
				data[0] = append(data[0], parquetInfo.recordtile[j])
			}
			//列内容
			for i := 0; i < len(parquetInfo.recordcontents)-1; i++ {
				for j := 0; j < len(parquetInfo.recordtile); j++ {
					tempcontent := parquetInfo.recordcontents[i][parquetInfo.recordtile[j]]
					if tempcontent != "" {
						data[i+1] = append(data[i+1], tempcontent)
					} else {
						data[i+1] = append(data[i+1], "")
					}

				}
			}
			//更新每列最长的内容数
			datalen = make([]int, len(parquetInfo.recordmaxlen))
			copy(datalen, parquetInfo.recordmaxlen)
			if len(datalen) > 0 {
				for j := 0; j < len(datalen); j++ {
					if datalen[j] > 0 {
						makeNavPara.viewtable.SetColumnWidth(j, float32(datalen[j]*10))
					}
				}
			}
			// fmt.Printf("%v\n", parquetInfo.recordmaxlen)
			// fmt.Printf("%v\n", datalen)
			dialogPro.Hide()

			setMiddleContent(makeNavPara.tableMiddle)
		}

	}, myWindow)
	fd.Resize(fyne.NewSize(800, 600))
	fd.SetFilter(storage.NewExtensionFileFilter([]string{".parquet"}))
	makeNavPara.fd = fd
	maincontent := container.NewBorder(
		container.NewVBox(title, widget.NewSeparator()), nil, nil, nil, content)
	if fyne.CurrentDevice().IsMobile() {
		myWindow.SetContent(makeNav(setMiddleContent, makeNavPara, myWindow, &parquetInfo))
	} else {
		split := container.NewHSplit(makeNav(setMiddleContent, makeNavPara, myWindow, &parquetInfo), maincontent)
		split.Offset = 0.2
		myWindow.SetContent(split)
	}

	myWindow.Resize(fyne.NewSize(1200, 900))
	myWindow.SetMaster()
	myWindow.Show()
	myApp.Run()
	closingUp()
}

func closingUp() {
	fmt.Println("Exited")
}

func parseURL(urlStr string) *url.URL {
	link, err := url.Parse(urlStr)
	if err != nil {
		fyne.LogError("Could not parse URL", err)
	}

	return link
}

func makeNav(setMiddleContent func(mc MiddleContent), para MakeNavPara, mWindow fyne.Window, parquetInfo *ParquetInfo) fyne.CanvasObject {

	setMiddleContent(para.welcomMiddle)

	buttons := container.New(layout.NewGridLayout(2),
		widget.NewButton("Open", func() {
			para.fd.Show()
		}),
		widget.NewButton("View", func() {
			if parquetInfo.filePath != "" {
				setMiddleContent(para.tableMiddle)
			} else {
				warningDlg := dialog.NewInformation("Info", "Please open the parquet file first", mWindow)
				warningDlg.Show()
			}

		}),
		widget.NewButton("Schema", func() {
			if parquetInfo.schemaName != "" {
				setMiddleContent(para.schemaMiddle)
			} else {
				warningDlg := dialog.NewInformation("Info", "Please open the parquet file first", mWindow)
				warningDlg.Show()
			}

		}),

		widget.NewButton("Export to CSV", func() {
			if parquetInfo.filePath != "" {
				dialogMou := dialog.NewCustom("Writing", "OK", container.New(layout.NewVBoxLayout(), widget.NewLabel("Please be patient, the data is writing"), widget.NewProgressBarInfinite()), mWindow)
				dialogMou.Show()
				filename := parquetInfo.schemaName + ".csv"
				columns := parseAllRecords(parquetInfo.filePath, parquetInfo)
				exportCsv(filename, *columns, mWindow)
				dialogMou.Hide()
				dialogFinish := dialog.NewInformation("Congratulations", "The file has been exported to "+filename, mWindow)
				dialogFinish.Show()
			} else {
				warningDlg := dialog.NewInformation("Info", "Please open the parquet file first", mWindow)
				warningDlg.Show()
			}

		}),
		widget.NewSeparator(),
		widget.NewButton("Exit", func() {
			a := fyne.CurrentApp()
			a.Quit()
		}),
	)

	infomation := container.New(layout.NewGridLayout(2),
		widget.NewLabel("Parquet Name:"),
		widget.NewLabelWithData(para.label_SchemaName),
		widget.NewLabel("Parquet Records:"),
		widget.NewLabelWithData(binding.IntToString(para.label_RecrodSum)),
		widget.NewLabel("Parquet Titles:"),
	)

	topcontent := container.NewVBox(buttons, infomation)

	schemaList := container.NewMax(
		widget.NewListWithData(para.list_SchemaList,
			func() fyne.CanvasObject {
				return widget.NewLabel("template")
			},
			func(i binding.DataItem, o fyne.CanvasObject) {
				o.(*widget.Label).Bind(i.(binding.String))
			}),
	)

	return container.NewBorder(topcontent, nil, nil, nil, topcontent, schemaList)
}

func exportCsv(filePath string, data [][]string, mWindow fyne.Window) {
	fp, err := os.Create(filePath) // 创建文件句柄
	if err != nil {
		dialog.ShowError(err, mWindow)
		// fmt.Printf("创建文件["+filePath+"]句柄失败,%v", err)
		return
	}
	defer fp.Close()

	fp.WriteString("\xEF\xBB\xBF") // 写入UTF-8 BOM
	w := csv.NewWriter(fp)         //创建一个新的写入文件流
	w.WriteAll(data)
	w.Flush()
}
