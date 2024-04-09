package export

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"time"

	"github.com/alist-org/alist/v3/drivers/base"
	"github.com/alist-org/alist/v3/internal/conf"
	"github.com/alist-org/alist/v3/internal/driver"
	"github.com/alist-org/alist/v3/internal/errs"
	"github.com/alist-org/alist/v3/internal/model"
	"github.com/alist-org/alist/v3/internal/stream"
	"github.com/alist-org/alist/v3/pkg/http_range"
	"github.com/alist-org/alist/v3/pkg/singleflight"
	"github.com/pkg/errors"
)

const baseDir = "/juicefs"
const RootName = "root"

var Storage driver.Driver

type FileSystem interface {
	Delete(ctx context.Context, name string) error
	Read(ctx context.Context, name string, off, limit int64) (io.ReadCloser, error)
	Put(ctx context.Context, name string, body io.Reader) error
}

type Impl struct{}

func New(ctx context.Context, addition string) (FileSystem, error) {
	conf.Conf = conf.DefaultConfig()
	base.InitClient()
	if err := json.Unmarshal([]byte(addition), Storage.GetAddition()); err != nil {
		return nil, err
	}
	if err := Storage.Init(ctx); err != nil {
		return nil, err
	}
	i := &Impl{}
	if err := i.mkdir(ctx, baseDir); err != nil {
		return nil, err
	}
	return i, nil
}

func (i *Impl) Delete(ctx context.Context, name string) error {
	rawObj, err := i.get(ctx, filepath.Join(baseDir, name))
	if err != nil {
		if errs.IsObjectNotFound(err) {
			return nil
		}
		return errors.WithMessage(err, "failed to get object")
	}

	switch s := Storage.(type) {
	case driver.Remove:
		err = s.Remove(ctx, model.UnwrapObj(rawObj))
	default:
		err = errs.NotImplement
	}
	return err
}

func (i *Impl) Read(ctx context.Context, name string, off, limit int64) (io.ReadCloser, error) {
	file, err := i.get(ctx, filepath.Join(baseDir, name))
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get file")
	}
	if file.IsDir() {
		return nil, errors.WithStack(errs.NotFile)
	}

	link, err := Storage.Link(ctx, file, model.LinkArgs{Header: http.Header{}})
	if err != nil {
		return nil, err
	}
	fs := stream.FileStream{
		Obj: file,
		Ctx: ctx,
	}
	// any link provided is seekable
	ss, err := stream.NewSeekableStream(fs, link)
	if err != nil {
		return nil, errors.WithMessagef(err, "failed get [%s] stream", file)
	}

	reader, err := ss.RangeRead(http_range.Range{Start: off, Length: limit})
	if err != nil {
		return nil, err
	}
	return io.NopCloser(reader), nil
}

func (i *Impl) Put(ctx context.Context, name string, body io.Reader) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	name = filepath.Join(baseDir, name)
	dir := filepath.Dir(name)
	realName := filepath.Base(name)

	obj := model.Object{
		Name:     realName,
		Size:     int64(len(data)),
		Modified: time.Now(),
		Ctime:    time.Now(),
	}

	stream := &stream.FileStream{
		Obj:    &obj,
		Reader: bytes.NewReader(data),
	}

	if err := i.mkdir(ctx, dir); err != nil {
		return errors.WithMessagef(err, "failed to make dir [%s]", baseDir)
	}

	parentDir, err := i.get(ctx, dir)
	if err != nil {
		return errors.WithMessagef(err, "failed to get dir [%s]", baseDir)
	}
	up := func(p float64) {}

	switch s := Storage.(type) {
	case driver.PutResult:
		_, err = s.Put(ctx, parentDir, stream, up)
	case driver.Put:
		err = s.Put(ctx, parentDir, stream, up)
	default:
		return errs.NotImplement
	}
	return errors.WithStack(err)
}

func (i *Impl) get(ctx context.Context, path string) (model.Obj, error) {
	// get the obj directly without list so that we can reduce the io
	if g, ok := Storage.(driver.Getter); ok {
		if path != baseDir {
			path = filepath.Join(baseDir, path)
		}
		obj, err := g.Get(ctx, path)
		if err == nil {
			return model.WrapObjName(obj), nil
		}
	}

	// is root folder
	if path == "/" {
		var rootObj model.Obj
		if getRooter, ok := Storage.(driver.GetRooter); ok {
			obj, err := getRooter.GetRoot(ctx)
			if err != nil {
				return nil, errors.WithMessage(err, "failed get root obj")
			}
			rootObj = obj
		} else {
			switch r := Storage.GetAddition().(type) {
			case driver.IRootId:
				rootObj = &model.Object{
					ID:       r.GetRootId(),
					Name:     RootName,
					Size:     0,
					Modified: Storage.GetStorage().Modified,
					IsFolder: true,
				}
			case driver.IRootPath:
				rootObj = &model.Object{
					Path:     r.GetRootPath(),
					Name:     RootName,
					Size:     0,
					Modified: Storage.GetStorage().Modified,
					IsFolder: true,
				}
			default:
				return nil, errors.Errorf("please implement IRootPath or IRootId or GetRooter method")
			}
		}
		if rootObj == nil {
			return nil, errors.Errorf("please implement IRootPath or IRootId or GetRooter method")
		}
		return &model.ObjWrapName{
			Name: RootName,
			Obj:  rootObj,
		}, nil
	}

	p := filepath.Dir(path)
	if p == "." {
		p = "/"
	}

	realName := filepath.Base(path)
	files, err := i.list(ctx, p, model.ListArgs{})
	if err != nil {
		return nil, errors.WithMessage(err, "failed get parent list")
	}
	for _, f := range files {
		if f.GetName() == realName {
			return f, nil
		}
	}
	return nil, errors.WithStack(errs.ObjectNotFound)
}

var listG singleflight.Group[[]model.Obj]

func (i *Impl) mkdir(ctx context.Context, dir string) error {
	p := filepath.Dir(dir)
	if p == "." {
		p = "/"
	}

	realDir := filepath.Base(dir)
	parent, err := i.get(ctx, p)
	if err != nil {
		if errs.IsObjectNotFound(err) {
			if err := i.mkdir(ctx, p); err != nil {
				return err
			}
			if parent, err = i.get(ctx, p); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	switch s := Storage.(type) {
	case driver.MkdirResult:
		_, err = s.MakeDir(ctx, parent, realDir)
	case driver.Mkdir:
		err = s.MakeDir(ctx, parent, realDir)
	default:
		return errs.NotImplement
	}
	return errors.WithStack(err)
}

func (i *Impl) list(ctx context.Context, dir string, args model.ListArgs) ([]model.Obj, error) {
	d, err := i.get(ctx, dir)
	if err != nil {
		return nil, err
	}
	if !d.IsDir() {
		return nil, errors.WithStack(errs.NotFolder)
	}
	objs, err, _ := listG.Do(dir, func() ([]model.Obj, error) {
		files, err := Storage.List(ctx, d, args)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to list objs")
		}
		// warp obj name
		model.WrapObjsName(files)
		return files, nil
	})
	return objs, err
}

