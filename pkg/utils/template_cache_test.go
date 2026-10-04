package utils_test

import (
	"text/template"

	"github.com/Masterminds/sprig/v3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/omq/pkg/utils"
)

func parse(s string) *template.Template {
	return template.Must(template.New("t").Funcs(sprig.FuncMap()).Parse(s))
}

var _ = Context("Template evaluation", func() {
	It("resolves static templates once per publisher id", func() {
		tmpl := parse("queue-%d")
		Expect(utils.ExecuteTemplate(tmpl, 3)).To(Equal("queue-3"))
		Expect(utils.ExecuteTemplate(tmpl, 7)).To(Equal("queue-7"))
		v, ok := utils.StaticTemplateValue(tmpl, 3)
		Expect(ok).To(BeTrue())
		Expect(v).To(Equal("queue-3"))
	})

	It("cycles through static comma-separated values using the sequence", func() {
		tmpl := parse("a, b ,c")
		Expect(utils.ExecuteTemplate(tmpl, 1, 0)).To(Equal("a"))
		Expect(utils.ExecuteTemplate(tmpl, 1, 1)).To(Equal("b"))
		Expect(utils.ExecuteTemplate(tmpl, 1, 5)).To(Equal("c"))
		_, ok := utils.StaticTemplateValue(tmpl, 1)
		Expect(ok).To(BeFalse())
	})

	It("still executes templates with actions", func() {
		tmpl := parse("id-{{.id}}")
		Expect(utils.ExecuteTemplate(tmpl, 9)).To(Equal("id-9"))
		_, ok := utils.StaticTemplateValue(tmpl, 9)
		Expect(ok).To(BeFalse())
		Expect(utils.ExecuteTemplate(parse("{{ add 1 2 }},x"), 1, 0)).To(Equal("3"))
	})
})

var _ = Context("BodyArena", func() {
	It("hands out independent copies", func() {
		var a utils.BodyArena
		src := []byte{1, 2, 3}
		b1 := a.Copy(src)
		b2 := a.Copy(src)
		b1[0] = 9
		Expect(b2[0]).To(Equal(byte(1)))
		Expect(src[0]).To(Equal(byte(1)))
		Expect(append(b1, 7)).NotTo(BeNil()) // capped capacity: append must not overwrite b2
		Expect(b2).To(Equal([]byte{1, 2, 3}))
	})
})
