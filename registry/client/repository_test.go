package client

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/manifest"
	"github.com/docker/distribution/manifest/schema1"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/api/errcode"
	"github.com/docker/distribution/registry/api/v2"
	"github.com/docker/distribution/testutil"
	"github.com/docker/distribution/uuid"
	"github.com/docker/libtrust"
	"github.com/opencontainers/go-digest"
)

func testServer(rrm testutil.RequestResponseMap) (string, func()) {
	h := testutil.NewHandler(rrm)
	s := httptest.NewServer(h)
	return s.URL, s.Close
}

// 生成blob的digest
func newRandomBlob(size int) (digest.Digest, []byte) {
	b := make([]byte, size)
	if n, err := rand.Read(b); err != nil {
		panic(err)
	} else if n != size {
		panic("unable to read enough bytes")
	}

	return digest.FromBytes(b), b
}

/* 测试pull layer */
func addTestFetch(repo string, dgst digest.Digest, content []byte, m *testutil.RequestResponseMap) {
	*m = append(*m, testutil.RequestResponseMapping{
		/* 生成请求的报头 */
		Request: testutil.Request{
			Method: "GET",
			/* digest用于定位具体的layer */
			Route: "/v2/" + repo + "/blobs/" + dgst.String(),
		},
		Response: testutil.Response{
			StatusCode: http.StatusOK,
			Body:       content,
			Headers: http.Header(map[string][]string{
				"Content-Length": {fmt.Sprint(len(content))},
				"Last-Modified":  {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
			}),
		},
	})

	/* HEAD请求用于检测目标registry中是否有对应的layer */
	*m = append(*m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: "HEAD",
			Route:  "/v2/" + repo + "/blobs/" + dgst.String(),
		},
		Response: testutil.Response{
			StatusCode: http.StatusOK,
			Headers: http.Header(map[string][]string{
				"Content-Length": {fmt.Sprint(len(content))},
				"Last-Modified":  {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
			}),
		},
	})
}

/* 列出本地registry集群中一组可用的存储库 */
func addTestCatalog(route string, content []byte, link string, m *testutil.RequestResponseMap) {
	headers := map[string][]string{
		"Content-Length": {strconv.Itoa(len(content))},
		"Content-Type":   {"application/json; charset=utf-8"},
	}
	if link != "" {
		headers["Link"] = append(headers["Link"], link)
	}
	/* 请求报头 GET /V2/_catalog */
	*m = append(*m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: "GET",
			Route:  route,
		},
		Response: testutil.Response{
			StatusCode: http.StatusOK,
			Body:       content,
			Headers:    http.Header(headers),
		},
	})
}

/* 删除指定存储库中的blob，使用digest来定位具体的blob */
func TestBlobDelete(t *testing.T) {
	/* 生成大小为1024blob的digest */
	dgst, _ := newRandomBlob(1024)
	var m testutil.RequestResponseMap
	repo, _ := reference.WithName("test.example.com/repo1")
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: "DELETE",
			Route:  "/v2/" + repo.Name() + "/blobs/" + dgst.String(),
		},
		Response: testutil.Response{
			StatusCode: http.StatusAccepted,
			Headers: http.Header(map[string][]string{
				"Content-Length": {"0"},
			}),
		},
	})

	e, c := testServer(m)
	defer c()
	/*  context用于追踪gorountine的上下文，
	Background是这个树结构最顶层的节点
	*/
	ctx := context.Background()
	/* 使用testServer生成的url创建一个新的存储库，用于测试 */
	r, err := NewRepository(repo, e, nil)
	if err != nil {
		t.Fatal(err)
	}
	/* 生成blobs结构，返回的数据结构l如下：
	blobs{
		name:    r.name,
		ub:      r.ub,
		client:  r.client,
		statter: cache.NewCachedBlobStatter(memory.NewInMemoryBlobDescriptorCacheProvider(), statter),
	}
	*/
	l := r.Blobs(ctx)
	/* 根据digest删除数据 */
	err = l.Delete(ctx, dgst)
	if err != nil {
		t.Errorf("Error deleting blob: %s", err.Error())
	}

}

/* 测试 pull blob*/
func TestBlobFetch(t *testing.T) {
	d1, b1 := newRandomBlob(1024)
	var m testutil.RequestResponseMap
	/* 调用#51的函数 */
	addTestFetch("test.example.com/repo1", d1, b1, &m)

	e, c := testServer(m)
	defer c()

	ctx := context.Background()
	repo, _ := reference.WithName("test.example.com/repo1")
	//创建目的registry的存储库
	r, err := NewRepository(repo, e, nil)
	if err != nil {
		t.Fatal(err)
	}
	/* 在目的存储库中创建blobstore */
	l := r.Blobs(ctx)
	/* Get方法的地址/Users/chenxu/Desktop/mygo/src/github.com/docker/distribution/registry/storage/blobstore.go
	Get方法获取存储在registry存储库中的数据，返回类型为[]byte
	*/
	b, err := l.Get(ctx, d1)
	if err != nil {
		t.Fatal(err)
	}
	/* 比较原数据大小和pull下来的数据大小是否一致 */
	if bytes.Compare(b, b1) != 0 {
		t.Fatalf("Wrong bytes values fetched: [%d]byte != [%d]byte", len(b), len(b1))
	}

	// TODO(dmcgowan): Test for unknown blob case
}

/* 测试blob在目标registry中是否存在,NoContentLength？ */
func TestBlobExistsNoContentLength(t *testing.T) {
	var m testutil.RequestResponseMap

	repo, _ := reference.WithName("biff")
	dgst, content := newRandomBlob(1024)
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: "GET",
			Route:  "/v2/" + repo.Name() + "/blobs/" + dgst.String(),
		},
		Response: testutil.Response{
			StatusCode: http.StatusOK,
			Body:       content,
			Headers: http.Header(map[string][]string{
				//			"Content-Length": {fmt.Sprint(len(content))},
				"Last-Modified": {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
			}),
		},
	})

	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: "HEAD",
			Route:  "/v2/" + repo.Name() + "/blobs/" + dgst.String(),
		},
		Response: testutil.Response{
			StatusCode: http.StatusOK,
			Headers: http.Header(map[string][]string{
				//			"Content-Length": {fmt.Sprint(len(content))},
				"Last-Modified": {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
			}),
		},
	})
	e, c := testServer(m)
	defer c()

	ctx := context.Background()
	r, err := NewRepository(repo, e, nil)
	if err != nil {
		t.Fatal(err)
	}
	l := r.Blobs(ctx)

	/* /Users/chenxu/Desktop/mygo/src/github.com/docker/distribution/registry/storage/blobstore.go
	stat继承了BlobStatter.Stat，返回blob的描述符，如果成功返回，则确保此blob存在并且可用
	*/
	_, err = l.Stat(ctx, dgst)
	if err == nil {
		t.Fatal(err)
	}
	if !strings.Contains(err.Error(), "missing content-length heade") {
		t.Fatalf("Expected missing content-length error message")
	}

}

/* 测试blob是否存在 */
func TestBlobExists(t *testing.T) {
	d1, b1 := newRandomBlob(1024)
	var m testutil.RequestResponseMap
	addTestFetch("test.example.com/repo1", d1, b1, &m)

	e, c := testServer(m)
	defer c()

	ctx := context.Background()
	repo, _ := reference.WithName("test.example.com/repo1")
	r, err := NewRepository(repo, e, nil)
	if err != nil {
		t.Fatal(err)
	}
	l := r.Blobs(ctx)

	/*  stat返回的格式如下：distribution.Descriptor{
		Size: fi.Size(),
		MediaType: "application/octet-stream",
		Digest:    dgst,
	}, nil
	*/
	stat, err := l.Stat(ctx, d1)
	if err != nil {
		t.Fatal(err)
	}

	/* 如果返回的digest和原来的不相同，报错 */
	if stat.Digest != d1 {
		t.Fatalf("Unexpected digest: %s, expected %s", stat.Digest, d1)
	}
	/* 大小不相同也报错 */
	if stat.Size != int64(len(b1)) {
		t.Fatalf("Unexpected length: %d, expected %d", stat.Size, len(b1))
	}

	// TODO(dmcgowan): Test error cases and ErrBlobUnknown case
}

/* 测试push 镜像 */
func TestBlobUploadChunked(t *testing.T) {
	dgst, b1 := newRandomBlob(1024)
	var m testutil.RequestResponseMap
	/* chunks将整个blob分成多个小块，用于上传 */
	chunks := [][]byte{
		b1[0:256],
		b1[256:512],
		b1[512:513],
		b1[513:1024],
	}
	//TODO:有待了解reference字段
	repo, _ := reference.WithName("test.example.com/uploadrepo")
	/* 生成唯一标志符 */
	uuids := []string{uuid.Generate().String()}
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			/* 第一步：POST请求用来获取一个url，这个url从Response中的，Location获取
			来执行第二步的实际上传操作 */
			Method: "POST",
			Route:  "/v2/" + repo.Name() + "/blobs/uploads/",
		},
		Response: testutil.Response{
			StatusCode: http.StatusAccepted,
			Headers: http.Header(map[string][]string{
				"Content-Length":     {"0"},
				"Location":           {"/v2/" + repo.Name() + "/blobs/uploads/" + uuids[0]},
				"Docker-Upload-UUID": {uuids[0]},
				"Range":              {"0-0"},
			}),
		},
	})
	offset := 0
	/* 为blob中的每一个chunk生成一个uuid，并且计算出位移量offset */
	for i, chunk := range chunks {
		uuids = append(uuids, uuid.Generate().String())
		newOffset := offset + len(chunk)
		m = append(m, testutil.RequestResponseMapping{
			Request: testutil.Request{
				/* 第二步（重复）：PATCH 执行chunk的上传操作 */
				Method: "PATCH",
				Route:  "/v2/" + repo.Name() + "/blobs/uploads/" + uuids[i],
				Body:   chunk,
			},
			Response: testutil.Response{
				StatusCode: http.StatusAccepted,
				Headers: http.Header(map[string][]string{
					"Content-Length":     {"0"},
					"Location":           {"/v2/" + repo.Name() + "/blobs/uploads/" + uuids[i+1]},
					"Docker-Upload-UUID": {uuids[i+1]},
					/* 表示下一个块的范围必须紧接在前一个响应的”最后有效范围“之后开始 */
					"Range": {fmt.Sprintf("%d-%d", offset, newOffset-1)},
				}),
			},
		})
		offset = newOffset
	}
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			/* 上传倒数第二步：要想上传过程被视为完成，上传最后一个chunk的同时必须要上传 digest */
			Method: "PUT",
			Route:  "/v2/" + repo.Name() + "/blobs/uploads/" + uuids[len(uuids)-1],
			QueryParams: map[string][]string{
				"digest": {dgst.String()},
			},
		},
		Response: testutil.Response{
			StatusCode: http.StatusCreated,
			Headers: http.Header(map[string][]string{
				/* registry接受到最后一个chunk并且layer被成功验证，
				client将会受到 201 Created 回复
				*/
				"Content-Length":        {"0"},
				"Docker-Content-Digest": {dgst.String()},
				"Content-Range":         {fmt.Sprintf("0-%d", offset-1)},
			}),
		},
	})
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			/* 最后一步：使用HEAD检验目标rregistry中是否存在此blob。 */
			Method: "HEAD",
			Route:  "/v2/" + repo.Name() + "/blobs/" + dgst.String(),
		},
		Response: testutil.Response{
			StatusCode: http.StatusOK,
			Headers: http.Header(map[string][]string{
				"Content-Length": {fmt.Sprint(offset)},
				"Last-Modified":  {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
			}),
		},
	})

	e, c := testServer(m)
	defer c()

	ctx := context.Background()
	r, err := NewRepository(repo, e, nil)
	if err != nil {
		t.Fatal(err)
	}
	l := r.Blobs(ctx)

	/*  Create 指定一个新的blob writer去添加blob到l中，
	可以写入返回的句柄，然后使用opaque恢复
	标识符。使用这种方法，可以关闭并恢复BlobWriter
	多次，直到BlobWriter被提交或取消。
	*/
	/*  TODO:找到l.create的具体实现。upload是一个blobwrite类型
		type BlobWriter interface {
		io.WriteCloser
		io.ReaderFrom

		// Size returns the number of bytes written to this blob.
		Size() int64

		// ID returns the identifier for this writer. The ID can be used with the
		// Blob service to later resume the write.
		ID() string

		// StartedAt returns the time this blob write was started.
		StartedAt() time.Time


		Commit(ctx context.Context, provisional Descriptor) (canonical Descriptor, err error)
		Cancel(ctx context.Context) error
	}
	*/
	upload, err := l.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if upload.ID() != uuids[0] {
		log.Fatalf("Unexpected UUID %s; expected %s", upload.ID(), uuids[0])
	}

	for _, chunk := range chunks {
		/* 开始上传数据，write的实现继承了标准的写接口，
		将数据写入到pipe中，阻塞直到这些数据被消费并且read端关闭。 */
		n, err := upload.Write(chunk)
		if err != nil {
			t.Fatal(err)
		}
		if n != len(chunk) {
			t.Fatalf("Unexpected length returned from write: %d; expected: %d", n, len(chunk))
		}
	}
	/* commit 完成blob writer的进程。并且通过提供的digest来验证这些上传的数据 */
	blob, err := upload.Commit(ctx, distribution.Descriptor{
		Digest: dgst,
		Size:   int64(len(b1)),
	})
	if err != nil {
		t.Fatal(err)
	}

	if blob.Size != int64(len(b1)) {
		t.Fatalf("Unexpected blob size: %d; expected: %d", blob.Size, len(b1))
	}
}

/* 单一传送，只传送一个chunk，与之前将一个blob等分成多个chunk相比
这种传输方式在具体实现上，只有一个uuid，请求的时候也是一个uuid而不是uuid数组。
剩下的流程和上面的上传过程相似。
*/
func TestBlobUploadMonolithic(t *testing.T) {
	dgst, b1 := newRandomBlob(1024)
	var m testutil.RequestResponseMap
	repo, _ := reference.WithName("test.example.com/uploadrepo")
	uploadID := uuid.Generate().String()
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: "POST",
			Route:  "/v2/" + repo.Name() + "/blobs/uploads/",
		},
		Response: testutil.Response{
			StatusCode: http.StatusAccepted,
			Headers: http.Header(map[string][]string{
				"Content-Length":     {"0"},
				"Location":           {"/v2/" + repo.Name() + "/blobs/uploads/" + uploadID},
				"Docker-Upload-UUID": {uploadID},
				"Range":              {"0-0"},
			}),
		},
	})
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: "PATCH",
			Route:  "/v2/" + repo.Name() + "/blobs/uploads/" + uploadID,
			Body:   b1,
		},
		Response: testutil.Response{
			StatusCode: http.StatusAccepted,
			Headers: http.Header(map[string][]string{
				"Location":              {"/v2/" + repo.Name() + "/blobs/uploads/" + uploadID},
				"Docker-Upload-UUID":    {uploadID},
				"Content-Length":        {"0"},
				"Docker-Content-Digest": {dgst.String()},
				"Range":                 {fmt.Sprintf("0-%d", len(b1)-1)},
			}),
		},
	})
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: "PUT",
			Route:  "/v2/" + repo.Name() + "/blobs/uploads/" + uploadID,
			QueryParams: map[string][]string{
				"digest": {dgst.String()},
			},
		},
		Response: testutil.Response{
			StatusCode: http.StatusCreated,
			Headers: http.Header(map[string][]string{
				"Content-Length":        {"0"},
				"Docker-Content-Digest": {dgst.String()},
				"Content-Range":         {fmt.Sprintf("0-%d", len(b1)-1)},
			}),
		},
	})
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: "HEAD",
			Route:  "/v2/" + repo.Name() + "/blobs/" + dgst.String(),
		},
		Response: testutil.Response{
			StatusCode: http.StatusOK,
			Headers: http.Header(map[string][]string{
				"Content-Length": {fmt.Sprint(len(b1))},
				"Last-Modified":  {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
			}),
		},
	})

	e, c := testServer(m)
	defer c()

	ctx := context.Background()
	r, err := NewRepository(repo, e, nil)
	if err != nil {
		t.Fatal(err)
	}
	l := r.Blobs(ctx)

	upload, err := l.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if upload.ID() != uploadID {
		log.Fatalf("Unexpected UUID %s; expected %s", upload.ID(), uploadID)
	}

	n, err := upload.ReadFrom(bytes.NewReader(b1))
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(b1)) {
		t.Fatalf("Unexpected ReadFrom length: %d; expected: %d", n, len(b1))
	}

	blob, err := upload.Commit(ctx, distribution.Descriptor{
		Digest: dgst,
		Size:   int64(len(b1)),
	})
	if err != nil {
		t.Fatal(err)
	}

	if blob.Size != int64(len(b1)) {
		t.Fatalf("Unexpected blob size: %d; expected: %d", blob.Size, len(b1))
	}
}

func TestBlobMount(t *testing.T) {
	dgst, content := newRandomBlob(1024)
	var m testutil.RequestResponseMap
	repo, _ := reference.WithName("test.example.com/uploadrepo")

	sourceRepo, _ := reference.WithName("test.example.com/sourcerepo")
	canonicalRef, _ := reference.WithDigest(sourceRepo, dgst)

	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method:      "POST",
			Route:       "/v2/" + repo.Name() + "/blobs/uploads/",
			QueryParams: map[string][]string{"from": {sourceRepo.Name()}, "mount": {dgst.String()}},
		},
		Response: testutil.Response{
			StatusCode: http.StatusCreated,
			Headers: http.Header(map[string][]string{
				"Content-Length":        {"0"},
				"Location":              {"/v2/" + repo.Name() + "/blobs/" + dgst.String()},
				"Docker-Content-Digest": {dgst.String()},
			}),
		},
	})
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: "HEAD",
			Route:  "/v2/" + repo.Name() + "/blobs/" + dgst.String(),
		},
		Response: testutil.Response{
			StatusCode: http.StatusOK,
			Headers: http.Header(map[string][]string{
				"Content-Length": {fmt.Sprint(len(content))},
				"Last-Modified":  {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
			}),
		},
	})

	e, c := testServer(m)
	defer c()

	ctx := context.Background()
	r, err := NewRepository(repo, e, nil)
	if err != nil {
		t.Fatal(err)
	}

	l := r.Blobs(ctx)

	bw, err := l.Create(ctx, WithMountFrom(canonicalRef))
	if bw != nil {
		t.Fatalf("Expected blob writer to be nil, was %v", bw)
	}

	if ebm, ok := err.(distribution.ErrBlobMounted); ok {
		if ebm.From.Digest() != dgst {
			t.Fatalf("Unexpected digest: %s, expected %s", ebm.From.Digest(), dgst)
		}
		if ebm.From.Name() != sourceRepo.Name() {
			t.Fatalf("Unexpected from: %s, expected %s", ebm.From.Name(), sourceRepo)
		}
	} else {
		t.Fatalf("Unexpected error: %v, expected an ErrBlobMounted", err)
	}
}

func newRandomSchemaV1Manifest(name reference.Named, tag string, blobCount int) (*schema1.SignedManifest, digest.Digest, []byte) {
	blobs := make([]schema1.FSLayer, blobCount)
	history := make([]schema1.History, blobCount)

	for i := 0; i < blobCount; i++ {
		dgst, blob := newRandomBlob((i % 5) * 16)

		blobs[i] = schema1.FSLayer{BlobSum: dgst}
		history[i] = schema1.History{V1Compatibility: fmt.Sprintf("{\"Hex\": \"%x\"}", blob)}
	}

	m := schema1.Manifest{
		Name:         name.String(),
		Tag:          tag,
		Architecture: "x86",
		FSLayers:     blobs,
		History:      history,
		Versioned: manifest.Versioned{
			SchemaVersion: 1,
		},
	}

	pk, err := libtrust.GenerateECP256PrivateKey()
	if err != nil {
		panic(err)
	}

	sm, err := schema1.Sign(&m, pk)
	if err != nil {
		panic(err)
	}

	return sm, digest.FromBytes(sm.Canonical), sm.Canonical
}

func addTestManifestWithEtag(repo reference.Named, reference string, content []byte, m *testutil.RequestResponseMap, dgst string) {
	actualDigest := digest.FromBytes(content)
	getReqWithEtag := testutil.Request{
		Method: "GET",
		Route:  "/v2/" + repo.Name() + "/manifests/" + reference,
		Headers: http.Header(map[string][]string{
			"If-None-Match": {fmt.Sprintf(`"%s"`, dgst)},
		}),
	}

	var getRespWithEtag testutil.Response
	if actualDigest.String() == dgst {
		getRespWithEtag = testutil.Response{
			StatusCode: http.StatusNotModified,
			Body:       []byte{},
			Headers: http.Header(map[string][]string{
				"Content-Length": {"0"},
				"Last-Modified":  {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
				"Content-Type":   {schema1.MediaTypeSignedManifest},
			}),
		}
	} else {
		getRespWithEtag = testutil.Response{
			StatusCode: http.StatusOK,
			Body:       content,
			Headers: http.Header(map[string][]string{
				"Content-Length": {fmt.Sprint(len(content))},
				"Last-Modified":  {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
				"Content-Type":   {schema1.MediaTypeSignedManifest},
			}),
		}

	}
	*m = append(*m, testutil.RequestResponseMapping{Request: getReqWithEtag, Response: getRespWithEtag})
}

func contentDigestString(mediatype string, content []byte) string {
	if mediatype == schema1.MediaTypeSignedManifest {
		m, _, _ := distribution.UnmarshalManifest(mediatype, content)
		content = m.(*schema1.SignedManifest).Canonical
	}
	return digest.Canonical.FromBytes(content).String()
}

func addTestManifest(repo reference.Named, reference string, mediatype string, content []byte, m *testutil.RequestResponseMap) {
	*m = append(*m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: "GET",
			Route:  "/v2/" + repo.Name() + "/manifests/" + reference,
		},
		Response: testutil.Response{
			StatusCode: http.StatusOK,
			Body:       content,
			Headers: http.Header(map[string][]string{
				"Content-Length":        {fmt.Sprint(len(content))},
				"Last-Modified":         {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
				"Content-Type":          {mediatype},
				"Docker-Content-Digest": {contentDigestString(mediatype, content)},
			}),
		},
	})
	*m = append(*m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: "HEAD",
			Route:  "/v2/" + repo.Name() + "/manifests/" + reference,
		},
		Response: testutil.Response{
			StatusCode: http.StatusOK,
			Headers: http.Header(map[string][]string{
				"Content-Length":        {fmt.Sprint(len(content))},
				"Last-Modified":         {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
				"Content-Type":          {mediatype},
				"Docker-Content-Digest": {digest.Canonical.FromBytes(content).String()},
			}),
		},
	})
}

func addTestManifestWithoutDigestHeader(repo reference.Named, reference string, mediatype string, content []byte, m *testutil.RequestResponseMap) {
	*m = append(*m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: "GET",
			Route:  "/v2/" + repo.Name() + "/manifests/" + reference,
		},
		Response: testutil.Response{
			StatusCode: http.StatusOK,
			Body:       content,
			Headers: http.Header(map[string][]string{
				"Content-Length": {fmt.Sprint(len(content))},
				"Last-Modified":  {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
				"Content-Type":   {mediatype},
			}),
		},
	})
	*m = append(*m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: "HEAD",
			Route:  "/v2/" + repo.Name() + "/manifests/" + reference,
		},
		Response: testutil.Response{
			StatusCode: http.StatusOK,
			Headers: http.Header(map[string][]string{
				"Content-Length": {fmt.Sprint(len(content))},
				"Last-Modified":  {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
				"Content-Type":   {mediatype},
			}),
		},
	})
}

func checkEqualManifest(m1, m2 *schema1.SignedManifest) error {
	if m1.Name != m2.Name {
		return fmt.Errorf("name does not match %q != %q", m1.Name, m2.Name)
	}
	if m1.Tag != m2.Tag {
		return fmt.Errorf("tag does not match %q != %q", m1.Tag, m2.Tag)
	}
	if len(m1.FSLayers) != len(m2.FSLayers) {
		return fmt.Errorf("fs blob length does not match %d != %d", len(m1.FSLayers), len(m2.FSLayers))
	}
	for i := range m1.FSLayers {
		if m1.FSLayers[i].BlobSum != m2.FSLayers[i].BlobSum {
			return fmt.Errorf("blobsum does not match %q != %q", m1.FSLayers[i].BlobSum, m2.FSLayers[i].BlobSum)
		}
	}
	if len(m1.History) != len(m2.History) {
		return fmt.Errorf("history length does not match %d != %d", len(m1.History), len(m2.History))
	}
	for i := range m1.History {
		if m1.History[i].V1Compatibility != m2.History[i].V1Compatibility {
			return fmt.Errorf("blobsum does not match %q != %q", m1.History[i].V1Compatibility, m2.History[i].V1Compatibility)
		}
	}
	return nil
}

func TestV1ManifestFetch(t *testing.T) {
	ctx := context.Background()
	repo, _ := reference.WithName("test.example.com/repo")
	m1, dgst, _ := newRandomSchemaV1Manifest(repo, "latest", 6)
	var m testutil.RequestResponseMap
	_, pl, err := m1.Payload()
	if err != nil {
		t.Fatal(err)
	}
	addTestManifest(repo, dgst.String(), schema1.MediaTypeSignedManifest, pl, &m)
	addTestManifest(repo, "latest", schema1.MediaTypeSignedManifest, pl, &m)
	addTestManifest(repo, "badcontenttype", "text/html", pl, &m)

	e, c := testServer(m)
	defer c()

	r, err := NewRepository(repo, e, nil)
	if err != nil {
		t.Fatal(err)
	}
	ms, err := r.Manifests(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ok, err := ms.Exists(ctx, dgst)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("Manifest does not exist")
	}

	manifest, err := ms.Get(ctx, dgst)
	if err != nil {
		t.Fatal(err)
	}
	v1manifest, ok := manifest.(*schema1.SignedManifest)
	if !ok {
		t.Fatalf("Unexpected manifest type from Get: %T", manifest)
	}

	if err := checkEqualManifest(v1manifest, m1); err != nil {
		t.Fatal(err)
	}

	var contentDigest digest.Digest
	manifest, err = ms.Get(ctx, dgst, distribution.WithTag("latest"), ReturnContentDigest(&contentDigest))
	if err != nil {
		t.Fatal(err)
	}
	v1manifest, ok = manifest.(*schema1.SignedManifest)
	if !ok {
		t.Fatalf("Unexpected manifest type from Get: %T", manifest)
	}

	if err = checkEqualManifest(v1manifest, m1); err != nil {
		t.Fatal(err)
	}

	if contentDigest != dgst {
		t.Fatalf("Unexpected returned content digest %v, expected %v", contentDigest, dgst)
	}

	manifest, err = ms.Get(ctx, dgst, distribution.WithTag("badcontenttype"))
	if err != nil {
		t.Fatal(err)
	}
	v1manifest, ok = manifest.(*schema1.SignedManifest)
	if !ok {
		t.Fatalf("Unexpected manifest type from Get: %T", manifest)
	}

	if err = checkEqualManifest(v1manifest, m1); err != nil {
		t.Fatal(err)
	}
}

func TestManifestFetchWithEtag(t *testing.T) {
	repo, _ := reference.WithName("test.example.com/repo/by/tag")
	_, d1, p1 := newRandomSchemaV1Manifest(repo, "latest", 6)
	var m testutil.RequestResponseMap
	addTestManifestWithEtag(repo, "latest", p1, &m, d1.String())

	e, c := testServer(m)
	defer c()

	ctx := context.Background()
	r, err := NewRepository(repo, e, nil)
	if err != nil {
		t.Fatal(err)
	}

	ms, err := r.Manifests(ctx)
	if err != nil {
		t.Fatal(err)
	}

	clientManifestService, ok := ms.(*manifests)
	if !ok {
		panic("wrong type for client manifest service")
	}
	_, err = clientManifestService.Get(ctx, d1, distribution.WithTag("latest"), AddEtagToTag("latest", d1.String()))
	if err != distribution.ErrManifestNotModified {
		t.Fatal(err)
	}
}

func TestManifestFetchWithAccept(t *testing.T) {
	ctx := context.Background()
	repo, _ := reference.WithName("test.example.com/repo")
	_, dgst, _ := newRandomSchemaV1Manifest(repo, "latest", 6)
	headers := make(chan []string, 1)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		headers <- req.Header["Accept"]
	}))
	defer close(headers)
	defer s.Close()

	r, err := NewRepository(repo, s.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	ms, err := r.Manifests(ctx)
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		// the media types we send
		mediaTypes []string
		// the expected Accept headers the server should receive
		expect []string
		// whether to sort the request and response values for comparison
		sort bool
	}{
		{
			mediaTypes: []string{},
			expect:     distribution.ManifestMediaTypes(),
			sort:       true,
		},
		{
			mediaTypes: []string{"test1", "test2"},
			expect:     []string{"test1", "test2"},
		},
		{
			mediaTypes: []string{"test1"},
			expect:     []string{"test1"},
		},
		{
			mediaTypes: []string{""},
			expect:     []string{""},
		},
	}
	for _, testCase := range testCases {
		ms.Get(ctx, dgst, distribution.WithManifestMediaTypes(testCase.mediaTypes))
		actual := <-headers
		if testCase.sort {
			sort.Strings(actual)
			sort.Strings(testCase.expect)
		}
		if !reflect.DeepEqual(actual, testCase.expect) {
			t.Fatalf("unexpected Accept header values: %v", actual)
		}
	}
}

func TestManifestDelete(t *testing.T) {
	repo, _ := reference.WithName("test.example.com/repo/delete")
	_, dgst1, _ := newRandomSchemaV1Manifest(repo, "latest", 6)
	_, dgst2, _ := newRandomSchemaV1Manifest(repo, "latest", 6)
	var m testutil.RequestResponseMap
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: "DELETE",
			Route:  "/v2/" + repo.Name() + "/manifests/" + dgst1.String(),
		},
		Response: testutil.Response{
			StatusCode: http.StatusAccepted,
			Headers: http.Header(map[string][]string{
				"Content-Length": {"0"},
			}),
		},
	})

	e, c := testServer(m)
	defer c()

	r, err := NewRepository(repo, e, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ms, err := r.Manifests(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if err := ms.Delete(ctx, dgst1); err != nil {
		t.Fatal(err)
	}
	if err := ms.Delete(ctx, dgst2); err == nil {
		t.Fatal("Expected error deleting unknown manifest")
	}
	// TODO(dmcgowan): Check for specific unknown error
}

func TestManifestPut(t *testing.T) {
	repo, _ := reference.WithName("test.example.com/repo/delete")
	m1, dgst, _ := newRandomSchemaV1Manifest(repo, "other", 6)

	_, payload, err := m1.Payload()
	if err != nil {
		t.Fatal(err)
	}

	var m testutil.RequestResponseMap
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: "PUT",
			Route:  "/v2/" + repo.Name() + "/manifests/other",
			Body:   payload,
		},
		Response: testutil.Response{
			StatusCode: http.StatusAccepted,
			Headers: http.Header(map[string][]string{
				"Content-Length":        {"0"},
				"Docker-Content-Digest": {dgst.String()},
			}),
		},
	})

	putDgst := digest.FromBytes(m1.Canonical)
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: "PUT",
			Route:  "/v2/" + repo.Name() + "/manifests/" + putDgst.String(),
			Body:   payload,
		},
		Response: testutil.Response{
			StatusCode: http.StatusAccepted,
			Headers: http.Header(map[string][]string{
				"Content-Length":        {"0"},
				"Docker-Content-Digest": {putDgst.String()},
			}),
		},
	})

	e, c := testServer(m)
	defer c()

	r, err := NewRepository(repo, e, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ms, err := r.Manifests(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := ms.Put(ctx, m1, distribution.WithTag(m1.Tag)); err != nil {
		t.Fatal(err)
	}

	if _, err := ms.Put(ctx, m1); err != nil {
		t.Fatal(err)
	}

	// TODO(dmcgowan): Check for invalid input error
}

func TestManifestTags(t *testing.T) {
	repo, _ := reference.WithName("test.example.com/repo/tags/list")
	tagsList := []byte(strings.TrimSpace(`
{
	"name": "test.example.com/repo/tags/list",
	"tags": [
		"tag1",
		"tag2",
		"funtag"
	]
}
	`))
	var m testutil.RequestResponseMap
	for i := 0; i < 3; i++ {
		m = append(m, testutil.RequestResponseMapping{
			Request: testutil.Request{
				Method: "GET",
				Route:  "/v2/" + repo.Name() + "/tags/list",
			},
			Response: testutil.Response{
				StatusCode: http.StatusOK,
				Body:       tagsList,
				Headers: http.Header(map[string][]string{
					"Content-Length": {fmt.Sprint(len(tagsList))},
					"Last-Modified":  {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
				}),
			},
		})
	}
	e, c := testServer(m)
	defer c()

	r, err := NewRepository(repo, e, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	tagService := r.Tags(ctx)

	tags, err := tagService.All(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tags) != 3 {
		t.Fatalf("Wrong number of tags returned: %d, expected 3", len(tags))
	}

	expected := map[string]struct{}{
		"tag1":   {},
		"tag2":   {},
		"funtag": {},
	}
	for _, t := range tags {
		delete(expected, t)
	}
	if len(expected) != 0 {
		t.Fatalf("unexpected tags returned: %v", expected)
	}
	// TODO(dmcgowan): Check for error cases
}

func TestObtainsErrorForMissingTag(t *testing.T) {
	repo, _ := reference.WithName("test.example.com/repo")

	var m testutil.RequestResponseMap
	var errors errcode.Errors
	errors = append(errors, v2.ErrorCodeManifestUnknown.WithDetail("unknown manifest"))
	errBytes, err := json.Marshal(errors)
	if err != nil {
		t.Fatal(err)
	}
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: "GET",
			Route:  "/v2/" + repo.Name() + "/manifests/1.0.0",
		},
		Response: testutil.Response{
			StatusCode: http.StatusNotFound,
			Body:       errBytes,
			Headers: http.Header(map[string][]string{
				"Content-Type": {"application/json; charset=utf-8"},
			}),
		},
	})
	e, c := testServer(m)
	defer c()

	ctx := context.Background()
	r, err := NewRepository(repo, e, nil)
	if err != nil {
		t.Fatal(err)
	}

	tagService := r.Tags(ctx)

	_, err = tagService.Get(ctx, "1.0.0")
	if err == nil {
		t.Fatalf("Expected an error")
	}
	if !strings.Contains(err.Error(), "manifest unknown") {
		t.Fatalf("Expected unknown manifest error message")
	}
}

func TestObtainsManifestForTagWithoutHeaders(t *testing.T) {
	repo, _ := reference.WithName("test.example.com/repo")

	var m testutil.RequestResponseMap
	m1, dgst, _ := newRandomSchemaV1Manifest(repo, "latest", 6)
	_, pl, err := m1.Payload()
	if err != nil {
		t.Fatal(err)
	}
	addTestManifestWithoutDigestHeader(repo, "1.0.0", schema1.MediaTypeSignedManifest, pl, &m)

	e, c := testServer(m)
	defer c()

	ctx := context.Background()
	r, err := NewRepository(repo, e, nil)
	if err != nil {
		t.Fatal(err)
	}

	tagService := r.Tags(ctx)

	desc, err := tagService.Get(ctx, "1.0.0")
	if err != nil {
		t.Fatalf("Expected no error")
	}
	if desc.Digest != dgst {
		t.Fatalf("Unexpected digest")
	}
}
func TestManifestTagsPaginated(t *testing.T) {
	s := httptest.NewServer(http.NotFoundHandler())
	defer s.Close()

	repo, _ := reference.WithName("test.example.com/repo/tags/list")
	tagsList := []string{"tag1", "tag2", "funtag"}
	var m testutil.RequestResponseMap
	for i := 0; i < 3; i++ {
		body, err := json.Marshal(map[string]interface{}{
			"name": "test.example.com/repo/tags/list",
			"tags": []string{tagsList[i]},
		})
		if err != nil {
			t.Fatal(err)
		}
		queryParams := make(map[string][]string)
		if i > 0 {
			queryParams["n"] = []string{"1"}
			queryParams["last"] = []string{tagsList[i-1]}
		}

		// Test both relative and absolute links.
		relativeLink := "/v2/" + repo.Name() + "/tags/list?n=1&last=" + tagsList[i]
		var link string
		switch i {
		case 0:
			link = relativeLink
		case len(tagsList) - 1:
			link = ""
		default:
			link = s.URL + relativeLink
		}

		headers := http.Header(map[string][]string{
			"Content-Length": {fmt.Sprint(len(body))},
			"Last-Modified":  {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
		})
		if link != "" {
			headers.Set("Link", fmt.Sprintf(`<%s>; rel="next"`, link))
		}

		m = append(m, testutil.RequestResponseMapping{
			Request: testutil.Request{
				Method:      "GET",
				Route:       "/v2/" + repo.Name() + "/tags/list",
				QueryParams: queryParams,
			},
			Response: testutil.Response{
				StatusCode: http.StatusOK,
				Body:       body,
				Headers:    headers,
			},
		})
	}

	s.Config.Handler = testutil.NewHandler(m)

	r, err := NewRepository(repo, s.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	tagService := r.Tags(ctx)

	tags, err := tagService.All(ctx)
	if err != nil {
		t.Fatal(tags, err)
	}
	if len(tags) != 3 {
		t.Fatalf("Wrong number of tags returned: %d, expected 3", len(tags))
	}

	expected := map[string]struct{}{
		"tag1":   {},
		"tag2":   {},
		"funtag": {},
	}
	for _, t := range tags {
		delete(expected, t)
	}
	if len(expected) != 0 {
		t.Fatalf("unexpected tags returned: %v", expected)
	}
}

func TestManifestUnauthorized(t *testing.T) {
	repo, _ := reference.WithName("test.example.com/repo")
	_, dgst, _ := newRandomSchemaV1Manifest(repo, "latest", 6)
	var m testutil.RequestResponseMap

	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: "GET",
			Route:  "/v2/" + repo.Name() + "/manifests/" + dgst.String(),
		},
		Response: testutil.Response{
			StatusCode: http.StatusUnauthorized,
			Body:       []byte("<html>garbage</html>"),
		},
	})

	e, c := testServer(m)
	defer c()

	r, err := NewRepository(repo, e, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ms, err := r.Manifests(ctx)
	if err != nil {
		t.Fatal(err)
	}

	_, err = ms.Get(ctx, dgst)
	if err == nil {
		t.Fatal("Expected error fetching manifest")
	}
	v2Err, ok := err.(errcode.Error)
	if !ok {
		t.Fatalf("Unexpected error type: %#v", err)
	}
	if v2Err.Code != errcode.ErrorCodeUnauthorized {
		t.Fatalf("Unexpected error code: %s", v2Err.Code.String())
	}
	if expected := errcode.ErrorCodeUnauthorized.Message(); v2Err.Message != expected {
		t.Fatalf("Unexpected message value: %q, expected %q", v2Err.Message, expected)
	}
}

func TestCatalog(t *testing.T) {
	var m testutil.RequestResponseMap
	addTestCatalog(
		"/v2/_catalog?n=5",
		[]byte("{\"repositories\":[\"foo\", \"bar\", \"baz\"]}"), "", &m)

	e, c := testServer(m)
	defer c()

	entries := make([]string, 5)

	r, err := NewRegistry(e, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	numFilled, err := r.Repositories(ctx, entries, "")
	if err != io.EOF {
		t.Fatal(err)
	}

	if numFilled != 3 {
		t.Fatalf("Got wrong number of repos")
	}
}

func TestCatalogInParts(t *testing.T) {
	var m testutil.RequestResponseMap
	addTestCatalog(
		"/v2/_catalog?n=2",
		[]byte("{\"repositories\":[\"bar\", \"baz\"]}"),
		"</v2/_catalog?last=baz&n=2>", &m)
	addTestCatalog(
		"/v2/_catalog?last=baz&n=2",
		[]byte("{\"repositories\":[\"foo\"]}"),
		"", &m)

	e, c := testServer(m)
	defer c()

	entries := make([]string, 2)

	r, err := NewRegistry(e, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	numFilled, err := r.Repositories(ctx, entries, "")
	if err != nil {
		t.Fatal(err)
	}

	if numFilled != 2 {
		t.Fatalf("Got wrong number of repos")
	}

	numFilled, err = r.Repositories(ctx, entries, "baz")
	if err != io.EOF {
		t.Fatal(err)
	}

	if numFilled != 1 {
		t.Fatalf("Got wrong number of repos")
	}
}

func TestSanitizeLocation(t *testing.T) {
	for _, testcase := range []struct {
		description string
		location    string
		source      string
		expected    string
		err         error
	}{
		{
			description: "ensure relative location correctly resolved",
			location:    "/v2/foo/baasdf",
			source:      "http://blahalaja.com/v1",
			expected:    "http://blahalaja.com/v2/foo/baasdf",
		},
		{
			description: "ensure parameters are preserved",
			location:    "/v2/foo/baasdf?_state=asdfasfdasdfasdf&digest=foo",
			source:      "http://blahalaja.com/v1",
			expected:    "http://blahalaja.com/v2/foo/baasdf?_state=asdfasfdasdfasdf&digest=foo",
		},
		{
			description: "ensure new hostname overridden",
			location:    "https://mwhahaha.com/v2/foo/baasdf?_state=asdfasfdasdfasdf",
			source:      "http://blahalaja.com/v1",
			expected:    "https://mwhahaha.com/v2/foo/baasdf?_state=asdfasfdasdfasdf",
		},
	} {
		fatalf := func(format string, args ...interface{}) {
			t.Fatalf(testcase.description+": "+format, args...)
		}

		s, err := sanitizeLocation(testcase.location, testcase.source)
		if err != testcase.err {
			if testcase.err != nil {
				fatalf("expected error: %v != %v", err, testcase)
			} else {
				fatalf("unexpected error sanitizing: %v", err)
			}
		}

		if s != testcase.expected {
			fatalf("bad sanitize: %q != %q", s, testcase.expected)
		}
	}
}
