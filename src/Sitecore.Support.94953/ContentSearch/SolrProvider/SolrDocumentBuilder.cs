
using System;
using System.Collections.Concurrent;

using Sitecore.ContentSearch.Diagnostics;
using Sitecore.Data.LanguageFallback;
using Sitecore.ContentSearch;
using Sitecore.StringExtensions;

namespace Sitecore.Support.ContentSearch.SolrProvider
{
  public class SolrDocumentBuilder : Sitecore.ContentSearch.SolrProvider.SolrDocumentBuilder
  {

    public SolrDocumentBuilder(IIndexable indexable, IProviderUpdateContext context) : base(indexable, context)
    {

    }

    public override void AddItemFields()
    {
      try
      {
        VerboseLogging.CrawlingLogDebug(() => "AddItemFields start");

        if (this.Options.IndexAllFields)
        {
          this.AddItemFieldsByItemList();
        }
        else
        {
          this.AddItemFieldsByIncludeList();
        }
      }
      finally
      {
        VerboseLogging.CrawlingLogDebug(() => "AddItemFields End");
      }
    }

    protected virtual void AddItemFieldsByItemList()
    {

    }

    protected virtual void AddItemFieldsByIncludeList()
    {

    }

    public override void AddField(IIndexableDataField field)
    {
      if (field.Name.IsNullOrEmpty())
      {
        CrawlingLog.Log.Warn("[Index={0}] '{1}' field of '{2}' item is skipped: the field name is missed."
          .FormatWith(this.Index.Name, field.Id, this.Indexable.Id));
        return;
      }

      base.AddField(field);
    }

    private void CheckAndAddField(IIndexable indexable, IIndexableDataField field)
    {
      var fieldKey = field.Name;

      if (this.IsTemplate && this.Options.HasExcludedTemplateFields)
      {
        if (this.Options.ExcludedTemplateFields.Contains(fieldKey) || this.Options.ExcludedTemplateFields.Contains(field.Id.ToString()))
        {
          VerboseLogging.CrawlingLogDebug(() => string.Format("Skipping field id:{0}, name:{1}, typeKey:{2} - Field was excluded.", field.Id, field.Name, field.TypeKey));
          return;
        }
      }

      if (this.IsMedia && this.Options.HasExcludedMediaFields)
      {
        if (this.Options.ExcludedMediaFields.Contains(field.Name))
        {
          VerboseLogging.CrawlingLogDebug(() => string.Format("Skipping field id:{0}, name:{1}, typeKey:{2} - Media field was excluded.", field.Id, field.Name, field.TypeKey));
          return;
        }
      }

      if (this.Options.ExcludedFields.Contains(field.Id.ToString()) || this.Options.ExcludedFields.Contains(fieldKey))
      {
        VerboseLogging.CrawlingLogDebug(() => string.Format("Skipping field id:{0}, name:{1}, typeKey:{2} - Field was excluded.", field.Id, field.Name, field.TypeKey));
        return;
      }

      try
      {
        if (this.Options.IndexAllFields)
        {
          using (new LanguageFallbackFieldSwitcher(this.Index.EnableFieldLanguageFallback))
          {
            this.AddField(field);
          }
        }
        else
        {
          if (this.Options.IncludedFields.Contains(fieldKey) || this.Options.IncludedFields.Contains(field.Id.ToString()))
          {
            using (new LanguageFallbackFieldSwitcher(this.Index.EnableFieldLanguageFallback))
            {
              this.AddField(field);
            }
          }
          else
          {
            VerboseLogging.CrawlingLogDebug(() => string.Format("Skipping field id:{0}, name:{1}, typeKey:{2} - Field was not included.", field.Id, field.Name, field.TypeKey));
          }
        }
      }
      catch (Exception ex)
      {
        if (!this.Settings.StopOnCrawlFieldError())
          CrawlingLog.Log.Fatal(string.Format("Could not add field {1} : {2} for indexable {0}", indexable.UniqueId, field.Id, field.Name), ex);
        else
          throw;
      }
    }
  }
}