impl DataStore {
    pub async fn quota_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::Quota> {
        opctx
            .authorize(authz::Action::ListChildren, authz::Resource::FLEET)
            .await?;
        let mut query = db::model::Quota::query();
        query = query.filter(db::schema::quotas::silo_id.eq(opctx.silo_id));
        query = query.order_by(db::schema::quotas::id.asc());
        query = pagparams.paginate(query);
        query
            .load_async::<db::model::Quota>(&self.pool)
            .await
            .map_err(Error::from)
    }
}
